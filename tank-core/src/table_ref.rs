use crate::{ColumnDef, Entity, quote_cow};
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
        if !self.schema.is_empty() {
            return format!("{}{}{}", self.schema, separator, self.name).into();
        }
        self.name.clone()
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

macro_rules! impl_from_for_table_ref {
    (ref $($ty:ty),* $(,)?) => {
        $(
            impl From<&$ty> for TableRef {
                fn from(value: &$ty) -> Self {
                    value.clone().into()
                }
            }

            impl From<&&$ty> for TableRef {
                fn from(value: &&$ty) -> Self {
                    (*value).clone().into()
                }
            }
        )*
    };
    ($($ty:ty),* $(,)?) => {
        $(
            impl From<$ty> for TableRef {
                fn from(value: $ty) -> Self {
                    TableRef::new(value.into())
                }
            }
        )*
        impl_from_for_table_ref!(ref TableRef, $($ty),*);
    };
}

impl_from_for_table_ref!(&'static str, String, Cow<'static, str>);

/// `(schema, name)` pair, as in `("analytics", "events")`.
impl From<(&'static str, &'static str)> for TableRef {
    fn from((schema, name): (&'static str, &'static str)) -> Self {
        TableRef {
            schema: Cow::Borrowed(schema),
            ..TableRef::new(Cow::Borrowed(name))
        }
    }
}

impl From<&(&'static str, &'static str)> for TableRef {
    fn from(value: &(&'static str, &'static str)) -> Self {
        (*value).into()
    }
}

impl<E: Entity> From<&E> for TableRef {
    fn from(_value: &E) -> Self {
        E::table().clone()
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
