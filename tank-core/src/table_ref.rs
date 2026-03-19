use crate::{
    ColumnDef, Dataset, DynQuery, quote_cow,
    writer::{Context, SqlWriter},
};
use proc_macro2::TokenStream;
use quote::{ToTokens, TokenStreamExt, quote};
use std::borrow::Cow;

/// Reference to a database table, including schema, alias name, columns, and primary key.
#[derive(Default, Clone, PartialEq, Eq, Hash, Debug)]
pub struct TableRef {
    /// Table name.
    pub name: Cow<'static, str>,
    /// Schema name.
    pub schema: Cow<'static, str>,
    /// Optional alias for the table.
    pub alias: Cow<'static, str>,
    /// List of columns defined in this table.
    pub columns: &'static [ColumnDef],
    /// List of columns forming the primary key.
    pub primary_key: &'static [&'static ColumnDef],
}

impl TableRef {
    /// Creates a new table reference with just a name.
    pub const fn new(name: Cow<'static, str>) -> Self {
        Self {
            name,
            schema: Cow::Borrowed(""),
            alias: Cow::Borrowed(""),
            columns: &[],
            primary_key: &[],
        }
    }
    /// Returns the fully qualified name (schema.table) or the alias if one is present.
    pub fn full_name(&self, separator: &str) -> Cow<'static, str> {
        if !self.alias.is_empty() {
            return self.alias.clone();
        }
        let mut name = self.name.clone();
        if !self.schema.is_empty() {
            name = format!("{}{}{}", self.schema, separator, name).into();
        }
        name
    }
    /// Returns a clone of the table reference with a new alias.
    pub fn with_alias(&self, alias: Cow<'static, str>) -> Self {
        let mut result = self.clone();
        result.alias = alias.into();
        result
    }
    /// Checks if the table reference has no name, schema, or alias.
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
