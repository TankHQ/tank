use crate::{
    DataSet, quote_cow,
    writer::{Context, SqlWriter},
};
use proc_macro2::TokenStream;
use quote::{ToTokens, TokenStreamExt, quote};
use std::borrow::Cow;

/// Schema-qualified table reference (optional alias).
#[derive(Default, Clone, PartialEq, Eq, Hash, Debug)]
pub struct TableRef {
    /// Table name.
    pub name: Cow<'static, str>,
    /// Schema name.
    pub schema: Cow<'static, str>,
    /// Optional alias used when rendering.
    pub alias: Cow<'static, str>,
}

impl TableRef {
    pub fn new(name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            name: name.into(),
            schema: "".into(),
            alias: "".into(),
        }
    }
    pub fn full_name(&self) -> String {
        let mut result = String::new();
        if !self.alias.is_empty() {
            result.push_str(&self.alias);
        } else {
            if !self.schema.is_empty() {
                result.push_str(&self.schema);
                result.push('.');
            }
            result.push_str(&self.name);
        }
        result
    }
    pub fn with_alias(&self, alias: Cow<'static, str>) -> Self {
        let mut result = self.clone();
        result.alias = alias.into();
        result
    }
    pub fn name<'s>(&'s self) -> &'s str {
        // TODO: replace with .as_str() and make the function const once https://github.com/rust-lang/rust/issues/130366 is stable
        &self.name
    }
    pub fn schema<'s>(&'s self) -> &'s str {
        // TODO: replace with .as_str() and make the function const once https://github.com/rust-lang/rust/issues/130366 is stable
        &self.schema
    }
    pub fn alias<'s>(&'s self) -> &'s str {
        // TODO: replace with .as_str() and make the function const once https://github.com/rust-lang/rust/issues/130366 is stable
        &self.alias
    }
}

impl DataSet for TableRef {
    fn qualified_columns() -> bool
    where
        Self: Sized,
    {
        false
    }
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut String) {
        writer.write_table_ref(context, out, self)
    }
}

impl DataSet for &TableRef {
    fn qualified_columns() -> bool
    where
        Self: Sized,
    {
        false
    }
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut String) {
        (*writer).write_table_ref(context, out, self)
    }
}

impl ToTokens for TableRef {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let name = &self.name;
        let schema = &self.schema;
        let alias = quote_cow(&self.alias);
        tokens.append_all(quote! {
            ::tank::TableRef {
                name: #name,
                schema: #schema,
                alias: #alias,
            }
        });
    }
}

/// Wrapper used when declaring table references in generated macros.
#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct DeclareTableRef(pub TableRef);

impl DataSet for DeclareTableRef {
    fn qualified_columns() -> bool
    where
        Self: Sized,
    {
        false
    }
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut String) {
        writer.write_table_ref(context, out, &self.0)
    }
}

impl ToTokens for DeclareTableRef {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let table_ref = &self.0;
        tokens.append_all(quote!(::tank::DeclareTableRef(#table_ref)));
    }
}
