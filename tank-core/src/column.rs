use crate::{
    DefaultValueType, DynQuery, Expression, ExpressionMatcher, OpPrecedence, SqlWriter, TableRef,
    Value, writer::Context,
};
use proc_macro2::TokenStream;
use quote::{ToTokens, TokenStreamExt, quote};
use std::{borrow::Cow, collections::BTreeMap};

/// Helper trait for types that expose an underlying column definition and reference.
pub trait ColumnTrait {
    /// Logical definition (column metadata).
    fn column_def(&self) -> &ColumnDef;
    /// Column reference to be used in expressions.
    fn column_ref(&self) -> &ColumnRef;
}

/// Reference to a table column.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct ColumnRef {
    /// Column name.
    pub name: Cow<'static, str>,
    /// Table name.
    pub table: Cow<'static, str>,
    /// Schema name (may be empty).
    pub schema: Cow<'static, str>,
}

impl ColumnRef {
    /// Return a `TableRef` referencing the column's table and schema.
    pub fn table(&self) -> TableRef {
        TableRef {
            name: self.table.clone(),
            schema: self.schema.clone(),
            ..Default::default()
        }
    }
}

/// Indicates if and how a column participates in the primary key.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum PrimaryKeyType {
    /// Single-column primary key.
    PrimaryKey,
    /// Member of a composite primary key.
    PartOfPrimaryKey,
    /// Not part of the primary key.
    #[default]
    None,
}

impl ToTokens for PrimaryKeyType {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        use PrimaryKeyType::*;
        tokens.append_all(match self {
            PrimaryKey => quote!(::tank::PrimaryKeyType::PrimaryKey),
            PartOfPrimaryKey => quote!(::tank::PrimaryKeyType::PartOfPrimaryKey),
            None => quote!(::tank::PrimaryKeyType::None),
        });
    }
}

/// Referential action for foreign key updates or deletes.
#[derive(Default, Debug, PartialEq, Eq)]
pub enum Action {
    /// No special action.
    #[default]
    NoAction,
    /// Reject the operation.
    Restrict,
    /// Propagate delete, update...
    Cascade,
    /// Set referencing columns to NULL.
    SetNull,
    /// Apply column DEFAULT.
    SetDefault,
}

impl ToTokens for Action {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.append_all(match self {
            Action::NoAction => quote! { ::tank::Action::NoAction },
            Action::Restrict => quote! { ::tank::Action::Restrict },
            Action::Cascade => quote! { ::tank::Action::Cascade },
            Action::SetNull => quote! { ::tank::Action::SetNull },
            Action::SetDefault => quote! { ::tank::Action::SetDefault },
        });
    }
}

/// Column specification.
#[derive(Default, Debug)]
pub struct ColumnDef {
    /// Column identity.
    pub column_ref: ColumnRef,
    /// Explicit SQL type override (empty means infer from `value`).
    pub column_type: BTreeMap<&'static str, &'static str>,
    /// `Value` describing column type and parameters.
    pub value: Value,
    /// Nullability flag.
    pub nullable: bool,
    /// Default value (expression rendered by `SqlWriter`).
    pub default: DefaultValueType,
    /// Primary key participation.
    pub primary_key: PrimaryKeyType,
    /// Defines the ordering of the rows.
    pub clustering_key: bool,
    /// Unique constraint (single column only, composite handled in the `TableDef`).
    pub unique: bool,
    /// Foreign key target column.
    pub references: Option<ColumnRef>,
    /// Action for deletes.
    pub on_delete: Option<Action>,
    /// Action for updates.
    pub on_update: Option<Action>,
    /// Passive columns are skipped when generating `INSERT` value lists (DEFAULT used).
    pub passive: bool,
    /// Optional human-readable comment.
    pub comment: &'static str,
}

impl ColumnDef {
    /// Column name (as declared in the table definition).
    pub fn name(&self) -> &str {
        &self.column_ref.name
    }
    /// Table name owning this column.
    pub fn table(&self) -> &str {
        &self.column_ref.table
    }
    /// Schema name owning this column (may be empty).
    pub fn schema(&self) -> &str {
        &self.column_ref.schema
    }
}

impl<'a> From<&'a ColumnDef> for &'a ColumnRef {
    fn from(value: &'a ColumnDef) -> Self {
        &value.column_ref
    }
}

impl OpPrecedence for ColumnRef {
    fn precedence(&self, _writer: &dyn SqlWriter) -> i32 {
        1_000_000
    }
}

impl Expression for ColumnRef {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_column_ref(context, out, self);
    }

    fn matches(&self, matcher: &mut dyn ExpressionMatcher) -> bool {
        matcher.match_column(self)
    }
}

impl OpPrecedence for ColumnDef {
    fn precedence(&self, _writer: &dyn SqlWriter) -> i32 {
        1_000_000
    }
}

impl Expression for ColumnDef {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_column_ref(context, out, &self.column_ref);
    }

    fn matches(&self, matcher: &mut dyn ExpressionMatcher) -> bool {
        matcher.match_column(&self.column_ref)
    }
}
