mod cols;
mod column_trait;
mod decode_column;
mod decode_expression;
mod decode_join;
mod decode_table;
mod encode_column_def;
mod encode_column_ref;
mod frag_evaluated;
mod from_row_trait;

use crate::{
    cols::ColList,
    decode_column::ColumnMetadata,
    decode_table::{TableMetadata, decode_table},
    encode_column_def::encode_column_def,
    from_row_trait::from_row_trait,
};
use column_trait::column_trait;
use decode_column::decode_column;
use decode_expression::decode_expression;
use decode_join::JoinParsed;
use frag_evaluated::flag_evaluated;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    Expr, Ident, Index, ItemStruct, parse_macro_input, parse2, punctuated::Punctuated,
    token::AndAnd,
};

#[proc_macro_derive(Entity, attributes(tank))]
pub fn derive_entity(input: TokenStream) -> TokenStream {
    let table = decode_table(parse_macro_input!(input as ItemStruct));
    let ident = &table.item.ident;
    let name = &table.name;
    let schema = &table.schema;
    let metadata_and_filter = table
        .columns
        .iter()
        .map(|metadata| {
            let filter_passive = if let Some(ref filter_passive) = metadata.check_passive {
                let field = &metadata.ident;
                filter_passive(quote!(self.#field))
            } else {
                quote!(true)
            };
            (metadata, filter_passive)
        })
        .collect::<Vec<_>>();
    let (from_row_factory, from_row) = from_row_trait(&table);
    let primary_key_cols = table.primary_key.iter().map(|i| &table.columns[*i]);
    let primary_key = primary_key_cols.clone().map(|col| {
        let ident = &col.ident;
        quote!(self.#ident)
    });
    let primary_keys_def = table.primary_key.iter().map(|i| quote!(&columns[#i]));
    let unique_defs = &table
        .unique
        .iter()
        .map(|v| {
            if v.is_empty() {
                quote!()
            } else {
                let i = v.iter();
                quote!(vec![#(&columns[#i]),*].into_boxed_slice())
            }
        })
        .collect::<Vec<_>>();
    let unique_defs = quote!(vec![#(#unique_defs),*].into_boxed_slice());
    let primary_key_types = primary_key_cols.clone().map(|col| col.ty.clone());
    let (column_trait, column) = column_trait(&table);
    let label_value_and_filter = metadata_and_filter.iter().map(|(column, filter)| {
        let name = &column.name;
        let field = &column.ident;
        quote!((#name.into(), ::tank::AsValue::as_value(self.#field.clone()), #filter))
    });
    let row_full = metadata_and_filter.iter().map(
        |(ColumnMetadata { ident, .. }, _)| quote!(::tank::AsValue::as_value(self.#ident.clone())),
    );
    let columns = metadata_and_filter.iter().map(|(c, _)| {
        let field = &c.ident;
        encode_column_def(&c, quote!(<#ident as #column_trait>::#field))
    });
    let primary_key_condition = table.primary_key.iter().enumerate().map(|(i, pki)| {
        let ident = table.columns[*pki].ident.clone();
        let span = ident.span();
        (ident, Ident::new(&format!("pk{}", i), span))
    });
    let primary_key_condition_declaration = primary_key_condition
        .clone()
        .enumerate()
        .clone()
        .map(|(i, (_, pk))| {
            let i = Index::from(i);
            quote! { let #pk = primary_key.#i.to_owned(); }
        })
        .collect::<TokenStream2>();
    let primary_key_condition_expression = primary_key_condition
        .clone()
        .map(|(field, pk)| quote!(#ident::#field == # #pk))
        .collect::<Punctuated<_, AndAnd>>();
    quote! {
        #from_row
        #column
        impl ::tank::Entity for #ident {
            type PrimaryKey<'a> = (#(&'a #primary_key_types,)*);

            fn table() -> &'static ::tank::TableRef {
                static TABLE: ::tank::TableRef = ::tank::TableRef {
                    name: ::std::borrow::Cow::Borrowed(#name),
                    schema: ::std::borrow::Cow::Borrowed(#schema),
                    alias: ::std::borrow::Cow::Borrowed(""),
                };
                &TABLE
            }

            fn columns() -> &'static [::tank::ColumnDef] {
                static RESULT: ::std::sync::LazyLock<Box<[::tank::ColumnDef]>> =
                    ::std::sync::LazyLock::new(|| vec![#(#columns),*].into_boxed_slice());
                &RESULT
            }

            fn primary_key_def() ->  &'static [&'static ::tank::ColumnDef] {
                static RESULT: ::std::sync::LazyLock<Box<[&'static ::tank::ColumnDef]>> =
                    ::std::sync::LazyLock::new(|| {
                        let columns = <#ident as ::tank::Entity>::columns();
                        vec![#(#primary_keys_def),*].into_boxed_slice()
                    });
                &RESULT
            }

            fn primary_key<'a>(&'a self) -> Self::PrimaryKey<'a> {
                (#(&#primary_key,)*)
            }

            fn unique_defs()
            -> impl ExactSizeIterator<Item = impl ExactSizeIterator<Item = &'static ::tank::ColumnDef>> {
                static RESULT: ::std::sync::LazyLock<Box<[Box<[&'static ::tank::ColumnDef]>]>> =
                    ::std::sync::LazyLock::new(|| {
                        let columns = #ident::columns();
                        #unique_defs
                    });
                RESULT.iter().map(|v| v.iter().copied())
            }

            fn row_filtered(&self) -> Box<[(&'static str, ::tank::Value)]> {
                [#(#label_value_and_filter),*]
                    .into_iter()
                    .filter_map(|(n, v, f)| if f { Some((n, v)) } else { None })
                    .collect()
            }

            fn row_full(&self) -> ::tank::Row {
                [#(#row_full),*].into()
            }

            fn from_row(row: ::tank::RowLabeled) -> ::tank::Result<Self> {
                #from_row_factory::<Self>::from_row(row)
            }

            fn find_pk(
                executor: &mut impl ::tank::Executor,
                primary_key: &Self::PrimaryKey<'_>,
            ) -> impl ::std::future::Future<Output = ::tank::Result<Option<Self>>> {
                async move {
                    #primary_key_condition_declaration
                    let condition = ::tank::expr!(#primary_key_condition_expression);
                    let query = ::tank::QueryBuilder::new()
                        .select(Self::columns())
                        .from(Self::table())
                        .where_condition(condition)
                        .limit(Some(1))
                        .build(&executor.driver());
                    // Replace StreamExt::boxed wrapper with ::std::pin::pin! once https://github.com/rust-lang/rust/issues/100013 is fixed
                    let mut stream = ::tank::stream::StreamExt::boxed(executor.fetch(query));
                    ::tank::stream::StreamExt::next(&mut stream)
                        .await
                        .map(|v| v.and_then(Self::from_row))
                        .transpose()
                    }
            }

            fn delete_one(
                executor: &mut impl ::tank::Executor,
                primary_key: Self::PrimaryKey<'_>,
            ) -> impl ::std::future::Future<Output = ::tank::Result<::tank::RowsAffected>> + Send
            where
                Self: Sized
            {
                #primary_key_condition_declaration
                let condition = ::tank::expr!(#primary_key_condition_expression);
                let mut query = ::tank::RawQuery::with_capacity(128);
                ::tank::SqlWriter::write_delete::<Self>(
                    &::tank::Driver::sql_writer(&executor.driver()),
                    &mut query,
                    &condition,
                );
                executor.execute(query)
            }
        }
    }
    .into()
}

#[proc_macro]
/// Build a typed join tree from a concise SQL-like syntax.
///
/// The grammar supports standard join variants (`JOIN`, `INNER JOIN`, `LEFT
/// JOIN`, `LEFT OUTER JOIN`, `RIGHT JOIN`, `RIGHT OUTER JOIN`, `FULL OUTER
/// JOIN`, `OUTER JOIN`, `CROSS`, `NATURAL JOIN`) plus nesting via parentheses
/// and chaining multiple joins in sequence. Optional `ON <expr>` clauses are
/// parsed into expressions using the same rules as [`expr!`].
///
/// Tables may be aliased by following them with an identifier (`MyTable MT
/// JOIN Other ON MT.id == Other.other_id`). Parentheses group joins when
/// building larger trees.
///
/// *Example*:
/// ```ignore
/// let books = join!(Book JOIN Author ON Book::author == Author::id)
///     .select(
///         executor,
///         cols!(Book::title, Author::name as author, Book::year),
///         &true,
///         None,
///     )
///     .and_then(|row| async { Books::from_row(row) })
///     .try_collect::<HashSet<_>>()
///     .await?;
/// ```
pub fn join(input: TokenStream) -> TokenStream {
    let result = parse_macro_input!(input as JoinParsed);
    result.0.into()
}

#[proc_macro]
/// Parse a Rust expression into a typed SQL expression tree.
///
/// The macro accepts a subset of Rust syntax with additional sentinel tokens for SQL semantics:
/// - `42`, `1.2`, `"Alpha"`, `true`, `NULL`, `[1, 2, 3]` literal values
/// - `#value` variable evaluation
/// - `RadioLog::signal_strength` column reference
/// - `Operator::id == #some_uuid` comparison: `==`, `!=`, `>`, `>=`. `<`, `<=`
/// - `!Operator::is_certified || RadioLog::signal_strength < -20` logical: `&&`, `||`, `!`
/// - `(a + b) * (c - d)` math operations: `+`, `-`, `*`, `/`, `%`
/// - `(flags >> 1) & 3` bitwise operations: `|`, `&`, `<<`, `>>`
/// - `[1, 2, 3][0]` array or map indexing
/// - `alpha == ? && beta > ?` prepared statement parameters
/// - `col == NULL`, `col != NULL` null check, it becomes `IS NULL` / `IS NOT NULL`
/// - `COUNT(*)`, `SUM(RadioLog::signal_strength)` function calls and aggregates
/// - `1 as u128` type casting
/// - `PI` identifiers
/// - `value != "ab%" as LIKE` pattern matching, it becomes `value NOT LIKE 'ab%'`,
///   it also supports `REGEXP` and `GLOB` (actual supports depends on the driver)
/// - `-(-PI) + 2 * (5 % (2 + 1)) == 7 && !(4 < 2)` combination of the previous
///
/// Parentheses obey standard Rust precedence.
/// Empty invocation (`expr!()`) yields `false`.
/// Ultimately, the drivers decide if and how these expressions are translated into the specific query language.
///
/// *Examples:*
/// ```ignore
/// use tank::expr;
/// let condition = expr!(User::age > 18 && User::active == true);
/// let rust_articles = expr!(Post::title == "Rust%" as LIKE);
/// let first_user = expr!(CAST(User::active as i32) == 1);
/// ```
pub fn expr(input: TokenStream) -> TokenStream {
    let mut input: TokenStream = flag_evaluated(input.into()).into();
    if input.is_empty() {
        input = quote!(false).into();
    }
    let expr = parse_macro_input!(input as Expr);
    let parsed = decode_expression(&expr);
    quote!(#parsed).into()
}

#[proc_macro]
/// Build a slice of column expressions (optionally ordered) suitable for a `SELECT` projection.
/// Each comma separated item becomes either an expression (parsed via [`expr!`]) or an ordered expression when followed by `ASC` or `DESC`.
///
/// Returns `&[&dyn Expression]` allowing direct passing to APIs expecting a
/// heterogeneous list of column expressions.
///
/// *Example*:
/// ```ignore
/// use tank::{cols, DataSet};
/// let names_stream = User::table().select(connection, cols!(User::id, User::name ASC), &true, Some(500));
/// let products_stream = Product::table().select(
///     executor,
///     cols!(COUNT(*), AVG(Product::price)),
///     &true,
///     None
/// );
/// ```
pub fn cols(input: TokenStream) -> TokenStream {
    let input = flag_evaluated(input.into());
    let Ok(ColList { cols: items }) = parse2(input) else {
        panic!("Could not parse the columns");
    };
    let generated = items.iter().map(|item| {
        let expr = &item.expr;
        match &item.order {
            Some(order) => {
                quote! {
                    ::tank::Ordered {
                        order: #order,
                        expression: ::tank::expr!(#expr),
                    }
                }
            }
            None => {
                quote! { ::tank::expr!(#expr) }
            }
        }
    });

    TokenStream::from(quote! {
        &[ #( &#generated as &dyn ::tank::Expression ),* ]
    })
}
