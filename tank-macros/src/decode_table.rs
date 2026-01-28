use crate::decode_column;
use crate::decode_column::ColumnMetadata;
use convert_case::{Case, Casing};
use proc_macro2::Span;
use quote::ToTokens;
use std::convert::identity;
use syn::spanned::Spanned;
use syn::{Error, Expr, ExprLit, ExprPath, ItemStruct, Lit, LitStr, Result, parse::ParseBuffer};
use tank_core::{PrimaryKeyType, matches_path};

pub(crate) struct TableMetadata {
    pub(crate) columns: Vec<ColumnMetadata>,
    pub(crate) name: String,
    pub(crate) item: ItemStruct,
    pub(crate) schema: String,
    pub(crate) primary_key: Vec<usize>,
    pub(crate) unique: Vec<Vec<usize>>,
}

fn decode_set_columns<'a, I: Iterator<Item = &'a ColumnMetadata> + Clone>(
    item: &ItemStruct,
    col: Expr,
    columns: I,
) -> Result<Vec<usize>> {
    Ok(match col {
        Expr::Lit(ExprLit {
            lit: Lit::Str(v), ..
        }) => {
            let v = v.value();
            let Some((i, _)) = columns.enumerate().find(|(_i, c)| c.name == v) else {
                return Err(Error::new(
                    v.span(),
                    format!("Column `{}` does not exist in the table", v),
                ));
            };
            vec![i]
        }
        Expr::Path(ExprPath { path, .. }) => {
            let Some((i, _)) = columns.enumerate().find(|(_i, c)| {
                let c = c.ident.to_string();
                matches_path(&path, &["Self", &c])
                    || matches_path(&path, &[&item.ident.to_string(), &c])
            }) else {
                return Err(Error::new(
                    path.span(),
                    format!(
                        "Field `{}` does not exist in the entity",
                        path.to_token_stream().to_string()
                    ),
                ));
            };
            vec![i]
        }
        Expr::Tuple(tuple) => {
            let elems: Vec<_> = tuple
                .elems
                .iter()
                .map(|v| decode_set_columns(&item, v.clone(), columns.clone()))
                .collect::<Result<_>>()?;
            if elems.iter().any(|v| v.len() != 1) {
                return Err(Error::new(
                    tuple.span(),
                    "Fields list inside tuple must either be a string literal column name or a column reference path",
                ));
            }
            elems.into_iter().flat_map(identity).collect()
        }
        _ => {
            return Err(Error::new(Span::call_site(), "Unexpected column set"));
        }
    })
}

pub fn decode_table(item: ItemStruct) -> TableMetadata {
    let mut columns: Vec<_> = item
        .fields
        .iter()
        .map(|f| decode_column(f))
        .filter(|c| !c.ignored)
        .collect();
    let mut name = item.ident.to_string().to_case(Case::Snake);
    let mut schema = String::new();
    let mut primary_key: Vec<_> = columns
        .iter()
        .enumerate()
        .filter_map(|(i, c)| {
            if c.primary_key != PrimaryKeyType::None {
                Some(i)
            } else {
                None
            }
        })
        .collect();
    let mut unique = vec![];
    if name.starts_with('_') {
        name.remove(0);
    }
    for attr in &item.attrs {
        let meta = &attr.meta;
        if meta.path().is_ident("tank") {
            let Ok(list) = meta.require_list() else {
                panic!("Error while parsing `tank`, use it like: `#[tank(attribute = value, ..)]`",);
            };
            let _ = list.parse_nested_meta(|arg| {
                if arg.path.is_ident("name") {
                    let Ok(value) = arg.value().and_then(ParseBuffer::parse::<LitStr>) else {
                        panic!(
                            "Error while parsing `name`, use it like: `#[tank(name = \"my_table\")]`"
                        );
                    };
                    name = value.value();
                } else if arg.path.is_ident("schema") {
                    let Ok(value) = arg.value().and_then(ParseBuffer::parse::<LitStr>) else {
                        panic!(
                            "Error while parsing `schema`, use it like: `#[tank(schema = \"my_schema\")]`"
                        );
                    };
                    schema = value.value();
                } else if arg.path.is_ident("primary_key") {
                    let cols_ids = arg
                        .value()
                        .and_then(ParseBuffer::parse::<Expr>)
                        .and_then(|v| decode_set_columns(&item, v, columns.iter()));
                    let cols_ids = match cols_ids {
                        Ok(v) => v,
                        Err(e) => {
                            panic!("Error while parsing `primary_key`, use it like: `#[tank(primary_key = (\"k1\", \"k2\", ..))]` ({e})");
                        }
                    };
                    if !primary_key.is_empty() {
                        panic!("Primary key attribute can appear just once on a table");
                    }
                    if let Some(column) = columns.iter().find(|c| c.primary_key != PrimaryKeyType::None) {
                        panic!(
                            "Column `{}` cannot be declared as a primary key while the table also specifies one",
                            column.name
                        )
                    }
                    primary_key = cols_ids
                } else if arg.path.is_ident("unique") {
                    let Ok(value) = arg
                        .value()
                        .and_then(ParseBuffer::parse::<Expr>)
                        .and_then(|v| decode_set_columns(&item, v, columns.iter())) else {
                        panic!("Error while parsing `unique`, use it like: `#[tank(unique = (\"k1\", \"k2\", ..))]`, you can specify more than one");
                    };
                    unique.push(value);
                } else {
                    panic!("Unknown attribute `{}` inside tank macro", arg.path.to_token_stream().to_string());
                }
                Ok(())
            });
        }
    }
    if !primary_key.is_empty() {
        let pk_type = if primary_key.len() == 1 {
            PrimaryKeyType::PrimaryKey
        } else {
            PrimaryKeyType::PartOfPrimaryKey
        };
        for pk in primary_key.iter() {
            columns[*pk].primary_key = pk_type;
        }
    }
    TableMetadata {
        columns,
        name,
        item,
        schema,
        primary_key,
        unique,
    }
}
