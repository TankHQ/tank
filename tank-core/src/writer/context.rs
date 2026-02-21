use crate::TableRef;
use std::borrow::Cow;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fragment {
    #[default]
    None,
    Aliasing,
    ParameterBinding,
    Casting,
    Json,
    JsonKey,
    SqlCommentOnColumn,
    SqlCreateSchema,
    SqlCreateTable,
    SqlCreateTablePrimaryKey,
    SqlCreateTableUnique,
    SqlDeleteFrom,
    SqlDeleteFromWhere,
    SqlDropSchema,
    SqlDropTable,
    SqlInsertInto,
    SqlInsertIntoOnConflict,
    SqlInsertIntoValues,
    SqlJoin,
    SqlSelect,
    SqlSelectFrom,
    SqlSelectGroupBy,
    SqlSelectHaving,
    SqlSelectOrderBy,
    SqlSelectWhere,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Context {
    pub counter: u32,
    pub fragment: Fragment,
    pub table_ref: TableRef,
    pub qualify_columns: bool,
    pub quote_identifiers: bool,
}

impl Context {
    pub const fn new(fragment: Fragment, qualify_columns: bool) -> Self {
        Self {
            counter: 0,
            fragment,
            table_ref: TableRef::new(Cow::Borrowed("")),
            qualify_columns,
            quote_identifiers: true,
        }
    }
    pub const fn empty() -> Self {
        Self {
            counter: 0,
            fragment: Fragment::None,
            table_ref: TableRef::new(Cow::Borrowed("")),
            qualify_columns: false,
            quote_identifiers: false,
        }
    }
    pub const fn fragment(fragment: Fragment) -> Self {
        Self {
            counter: 0,
            fragment,
            table_ref: TableRef::new(Cow::Borrowed("")),
            qualify_columns: false,
            quote_identifiers: true,
        }
    }
    pub const fn qualify(qualify_columns: bool) -> Self {
        Self {
            counter: 0,
            fragment: Fragment::None,
            table_ref: TableRef::new(Cow::Borrowed("")),
            qualify_columns,
            quote_identifiers: true,
        }
    }
    pub const fn qualify_with(table: Cow<'static, str>) -> Self {
        Self {
            counter: 0,
            fragment: Fragment::None,
            table_ref: TableRef::new(table),
            qualify_columns: true,
            quote_identifiers: true,
        }
    }
    pub const fn update_from(&mut self, context: &Context) {
        self.counter = context.counter;
    }
    pub fn switch_fragment<'s>(&'s mut self, fragment: Fragment) -> ContextUpdater<'s> {
        ContextUpdater {
            current: Context {
                fragment,
                table_ref: self.table_ref.clone(),
                ..*self
            },
            previous: self,
        }
    }
    pub fn switch_table<'s>(&'s mut self, table_ref: TableRef) -> ContextUpdater<'s> {
        let is_empty = table_ref.is_empty();
        ContextUpdater {
            current: Context {
                table_ref,
                qualify_columns: !is_empty,
                ..*self
            },
            previous: self,
        }
    }
    pub fn is_inside_json(&self) -> bool {
        self.fragment == Fragment::Json || self.fragment == Fragment::JsonKey
    }
}

impl Default for Context {
    fn default() -> Self {
        Context::new(Fragment::None, true)
    }
}

pub struct ContextUpdater<'a> {
    pub current: Context,
    pub previous: &'a mut Context,
}

impl<'a> Drop for ContextUpdater<'a> {
    fn drop(&mut self) {
        self.previous.counter = self.current.counter;
    }
}
