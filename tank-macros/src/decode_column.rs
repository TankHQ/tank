use proc_macro2::TokenStream;
use quote::ToTokens;
use std::{collections::BTreeMap, fmt::Debug};
use syn::{
    Expr, ExprCall, ExprLit, ExprMethodCall, Field, Ident, Lit, LitStr, Path, Result, Type,
    custom_keyword, parenthesized,
    parse::{Parse, ParseStream},
    parse2,
    token::{Comma, Eq},
};
use tank_core::{
    Action, CheckPassive, PrimaryKeyType, TypeDecoded, Value, decode_type, future::Either,
};

pub(crate) struct ColumnMetadata {
    pub(crate) ident: Ident,
    pub(crate) ignored: bool,
    pub(crate) ty: Type,
    pub(crate) name: String,
    pub(crate) column_type: BTreeMap<String, String>,
    pub(crate) value: Value,
    pub(crate) nullable: bool,
    pub(crate) default: Option<TokenStream>,
    pub(crate) primary_key: PrimaryKeyType,
    pub(crate) clustering_key: bool,
    pub(crate) references: Option<Either<TokenStream, (String, String)>>,
    pub(crate) on_delete: Option<Action>,
    pub(crate) on_update: Option<Action>,
    pub(crate) unique: bool,
    pub(crate) passive: bool,
    pub(crate) check_passive: Option<CheckPassive>,
    pub(crate) comment: String,
}

impl Debug for ColumnMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnMetadata")
            .field("ident", &self.ident)
            .field("ty", &"..")
            .field("name", &self.name)
            .field("column_type", &self.column_type)
            .field("value", &self.value)
            .field("nullable", &self.nullable)
            .field("default", &self.default)
            .field("primary_key", &self.primary_key)
            .field("clustering_key", &self.clustering_key)
            .field("references", &self.references)
            .field("on_delete", &self.on_delete)
            .field("on_update", &self.on_update)
            .field("unique", &self.unique)
            .field("passive", &self.passive)
            .field("check_passive", &"..")
            .field("comment", &self.comment)
            .finish()
    }
}

#[derive(Debug)]
struct Entry {
    name: String,
    value: TokenStream,
}

impl Parse for Entry {
    fn parse(input: ParseStream) -> Result<Self> {
        let ident: syn::Ident = input.parse()?;
        let name = ident.to_string();
        let value = if input.parse::<Eq>().is_ok() {
            input
                .parse::<TokenStream>()
                .expect("There must be some value after `=`")
        } else {
            TokenStream::new()
        };
        Ok(Entry { name, value })
    }
}

#[derive(Debug)]
struct Entries(pub(crate) Vec<Entry>);

impl Parse for Entries {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(Entries(
            input
                .parse_terminated(Expr::parse, Comma)?
                .into_iter()
                .map(ToTokens::into_token_stream)
                .map(parse2::<Entry>)
                .flatten()
                .collect(),
        ))
    }
}

struct TypeEntries {
    types: BTreeMap<String, String>,
}

impl Parse for TypeEntries {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut map = BTreeMap::new();
        let content;
        parenthesized!(content in input);
        for pair in content.parse_terminated(TypeEntry::parse, Comma)? {
            map.insert(pair.key, pair.value);
        }
        Ok(TypeEntries { types: map })
    }
}

struct TypeEntry {
    key: String,
    value: String,
}

impl Parse for TypeEntry {
    fn parse(input: ParseStream) -> Result<Self> {
        let key = input.parse::<Ident>()?.to_string();
        input.parse::<Eq>()?;
        let value_lit = input.parse::<LitStr>()?;
        Ok(TypeEntry {
            key,
            value: value_lit.value(),
        })
    }
}

pub fn decode_column(field: &Field) -> ColumnMetadata {
    let ident = field
        .ident
        .clone()
        .expect("Field is expected to have a name");
    let name = ident.to_string();
    let mut metadata = ColumnMetadata {
        ident,
        ignored: false,
        ty: field.ty.clone(),
        name,
        column_type: Default::default(),
        value: Value::Null,
        nullable: false,
        default: None,
        primary_key: PrimaryKeyType::None,
        clustering_key: false,
        references: None,
        on_delete: None,
        on_update: None,
        unique: false,
        passive: false,
        check_passive: None,
        comment: String::new(),
    };
    if metadata.name.starts_with('_') {
        metadata.name.remove(0);
    }
    for attr in &field.attrs {
        let meta = &attr.meta;
        if meta.path().is_ident("tank") {
            let Ok(list) = meta.require_list() else {
                panic!("Cannot parse `tank`, example: `#[tank(attribute = value, ..)]`");
            };
            let entries = parse2::<Entries>(list.tokens.clone()).expect("...").0;
            for entry in entries {
                let (name, value) = (entry.name, entry.value);
                if name == "ignore" {
                    metadata.ignored = true;
                } else if name == "default" {
                    metadata.default = Some(value.to_token_stream().into());
                } else if name == "name" {
                    let Ok(v) = parse2::<LitStr>(value.clone()) else {
                        panic!("Cannot parse `name`, example: `#[tank(name = \"my_column\")]`");
                    };
                    metadata.name = v.value();
                } else if name == "column_type" {
                    let column_type = parse2::<TypeEntries>(value.clone())
                        .expect("Cannot parse `column_type`, example: `#[tank(column_type = (postgres = \"TEXT\", mysql = \"VARCHAR(255)\")]`");
                    metadata.column_type = column_type.types;
                } else if name == "primary_key" {
                    metadata.primary_key = PrimaryKeyType::PrimaryKey;
                    metadata.nullable = false;
                } else if name == "clustering_key" {
                    metadata.clustering_key = true;
                    metadata.nullable = false;
                } else if name == "references" {
                    let reference = if let Ok(v) = parse2::<ExprMethodCall>(value.clone()) {
                        if v.args.len() != 1 {
                            panic!("Expected references to have a single argument");
                        }
                        let receiver = v.receiver.to_token_stream();
                        let method = v.method.to_token_stream().to_string();
                        let arg = v.args.first().unwrap().into_token_stream().to_string();
                        Either::Right((format!("{}.{}", receiver, method), arg))
                    } else if let Ok(v) = parse2::<ExprCall>(value.clone()) {
                        if v.args.len() != 1 {
                            panic!("Expected references to have a single argument");
                        }
                        let function = v.func.to_token_stream().to_string();
                        let arg = v.args.first().unwrap().into_token_stream().to_string();
                        Either::Right((function, arg))
                    } else if let Ok(v) = parse2::<Path>(value.clone()) {
                        Either::Left(v.to_token_stream())
                    } else {
                        panic!(
                            "Unexpected expression syntax for `references` {:?}, use it like: `MyEntity::column` or `schema.table_name(column_name)`",
                            value.to_string()
                        );
                    };
                    metadata.references = Some(reference);
                } else if name == "on_delete" || name == "on_update" {
                    let is_delete = name == "on_delete";
                    custom_keyword!(no_action);
                    custom_keyword!(restrict);
                    custom_keyword!(cascade);
                    custom_keyword!(set_null);
                    custom_keyword!(set_default);
                    let action = if let Ok(..) = parse2::<no_action>(value.clone()) {
                        Action::NoAction
                    } else if let Ok(..) = parse2::<restrict>(value.clone()) {
                        Action::Restrict
                    } else if let Ok(..) = parse2::<cascade>(value.clone()) {
                        Action::Cascade
                    } else if let Ok(..) = parse2::<set_null>(value.clone()) {
                        Action::SetNull
                    } else if let Ok(..) = parse2::<set_default>(value.clone()) {
                        Action::SetDefault
                    } else {
                        panic!(
                            "Expected the action to be either no_action, restrict, cascade, set_null, set_default"
                        );
                    };
                    if is_delete {
                        metadata.on_delete = action.into();
                    } else {
                        metadata.on_update = action.into();
                    }
                } else if name == "unique" {
                    metadata.unique = true;
                } else {
                    panic!("Unknown attribute `{}` inside tank macro", name);
                }
            }
        } else if meta.path().is_ident("doc") {
            let Ok(&Expr::Lit(ExprLit {
                lit: Lit::Str(ref v),
                ..
            })) = meta.require_name_value().and_then(|v| Ok(&v.value))
            else {
                panic!("Error while parsing the comment, use it like: `/// Column comment");
            };
            if !metadata.comment.is_empty() {
                metadata.comment.push('\n');
            }
            metadata.comment.push_str(v.value().trim());
        }
    }
    if !metadata.ignored {
        let (
            TypeDecoded {
                value,
                nullable,
                passive,
            },
            check_passive,
        ) = if let Type::Path(..) = &field.ty {
            decode_type(&field.ty)
        } else if let Type::Array(..) = &field.ty {
            decode_type(&field.ty)
        } else {
            Default::default()
        };
        metadata.value = value;
        metadata.nullable = nullable;
        metadata.passive = passive;
        metadata.check_passive = check_passive;
    }
    metadata
}
