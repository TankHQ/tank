use crate::{
    DefaultValueType, DynQuery, Expression, ExpressionVisitor, OpPrecedence, SqlWriter, TableRef,
    Value, writer::Context,
};
use proc_macro2::TokenStream;
use quote::{ToTokens, TokenStreamExt, quote};
use std::{borrow::Cow, collections::BTreeMap, hash::Hash};

/// Trait exposing column definition and reference.
pub trait ColumnTrait {
    /// Logical definition (column metadata).
    fn column_def(&self) -> &ColumnDef;
    /// Column reference.
    fn column_ref(&self) -> &ColumnRef;
}

/// Reference to a column.
#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// Name of the column.
    pub name: Cow<'static, str>,
    /// Name of the table.
    pub table: Cow<'static, str>,
    /// Name of the schema.
    pub schema: Cow<'static, str>,
}

impl ColumnRef {
    pub fn new(name: Cow<'static, str>) -> Self {
        Self {
            name,
            ..Default::default()
        }
    }
    /// Get the table reference for this column.
    pub fn table(&self) -> TableRef {
        TableRef {
            name: self.table.clone(),
            schema: self.schema.clone(),
            ..Default::default()
        }
    }
}

/// Primary key participation.
#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
pub enum PrimaryKeyType {
    /// Full primary key.
    PrimaryKey,
    /// Part of a composite key.
    PartOfPrimaryKey,
    /// Not in primary key.
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
    /// No action.
    #[default]
    NoAction,
    /// Reject operation.
    Restrict,
    /// Propagate change.
    Cascade,
    /// Set to NULL.
    SetNull,
    /// Set to DEFAULT.
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
    /// Explicit SQL types.
    pub column_type: BTreeMap<&'static str, &'static str>,
    /// Type descriptor.
    pub value: Value,
    /// Is nullable.
    pub nullable: bool,
    /// Default expression.
    pub default: DefaultValueType,
    /// Primary key role.
    pub primary_key: PrimaryKeyType,
    /// Clustering key (relevant for ScyllaDB / Cassandra).
    pub clustering_key: bool,
    /// Single-column unique constraint.
    pub unique: bool,
    /// Foreign key target.
    pub references: Option<ColumnRef>,
    /// On delete action.
    pub on_delete: Option<Action>,
    /// On update action.
    pub on_update: Option<Action>,
    /// Exclude from INSERTs.
    pub passive: bool,
    /// Comment.
    pub comment: &'static str,
}

impl ColumnDef {
    /// Column name.
    pub fn name(&self) -> &str {
        &self.column_ref.name
    }
    /// Table name.
    pub fn table(&self) -> &str {
        &self.column_ref.table
    }
    /// Schema name.
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
    fn accept_visitor(
        &self,
        matcher: &mut dyn ExpressionVisitor,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
    ) -> bool {
        matcher.visit_column(writer, context, out, self)
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

    fn accept_visitor(
        &self,
        matcher: &mut dyn ExpressionVisitor,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
    ) -> bool {
        matcher.visit_column(writer, context, out, &self.column_ref)
    }
}

impl PartialEq for ColumnDef {
    fn eq(&self, other: &Self) -> bool {
        self.column_ref == other.column_ref
    }
}

impl Eq for ColumnDef {}

impl Hash for ColumnDef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.column_ref.hash(state)
    }
}
