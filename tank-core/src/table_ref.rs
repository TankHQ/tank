use crate::{
    Dataset, DynQuery, quote_cow,
    writer::{Context, SqlWriter},
};
use proc_macro2::TokenStream;
use quote::{ToTokens, TokenStreamExt, quote};
use std::borrow::Cow;

/// Table reference.
#[derive(Default, Clone, PartialEq, Eq, Hash, Debug)]
pub struct TableRef {
    /// Table name.
    pub name: Cow<'static, str>,
    /// Schema name.
    pub schema: Cow<'static, str>,
    /// Alias.
    pub alias: Cow<'static, str>,
}

impl TableRef {
    /// New table reference.
    pub const fn new(name: Cow<'static, str>) -> Self {
        Self {
            name,
            schema: Cow::Borrowed(""),
            alias: Cow::Borrowed(""),
        }
    }
    /// Get the display name.
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
    /// Set the alias.
    pub fn with_alias(&self, alias: Cow<'static, str>) -> Self {
        let mut result = self.clone();
        result.alias = alias.into();
        result
    }
    /// True if empty.
    pub fn is_empty(&self) -> bool {
        self.name.is_empty() && self.schema.is_empty() && self.alias.is_empty()
    }
}

impl Dataset for TableRef {
    fn qualified_columns() -> bool
    where
        Self: Sized,
    {
        false
    }
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_table_ref(context, out, self)
    }
    fn table_ref(&self) -> TableRef {
        self.clone()
    }
}

impl Dataset for &TableRef {
    fn qualified_columns() -> bool
    where
        Self: Sized,
    {
        false
    }
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        (*writer).write_table_ref(context, out, self)
    }
    fn table_ref(&self) -> TableRef {
        (*self).clone()
    }
}

impl From<&'static str> for TableRef {
    fn from(value: &'static str) -> Self {
        TableRef::new(value.into())
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

impl Dataset for DeclareTableRef {
    fn qualified_columns() -> bool
    where
        Self: Sized,
    {
        false
    }
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_table_ref(context, out, &self.0)
    }
    fn table_ref(&self) -> TableRef {
        self.0.clone()
    }
}

impl ToTokens for DeclareTableRef {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let table_ref = &self.0;
        tokens.append_all(quote!(::tank::DeclareTableRef(#table_ref)));
    }
}
