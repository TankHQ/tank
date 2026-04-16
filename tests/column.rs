#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use tank::{ColumnRef, Entity};

    #[test]
    fn test_column_conversions() {
        #[derive(Entity)]
        #[tank(schema = "the_schema", name = "my_table")]
        struct Entity {
            #[tank(name = "solo_column")]
            col: i32,
        }

        let column = &Entity::columns()[0];
        assert_eq!(column.name(), "solo_column");
        assert_eq!(column.table(), "my_table");
        assert_eq!(column.schema(), "the_schema");

        let col_ref: &ColumnRef = column.into();
        assert_eq!(col_ref.name, "solo_column");
        assert_eq!(col_ref.table, "my_table");
        assert_eq!(col_ref.schema, "the_schema");

        let my_column = ColumnRef::new("my_column".into());
        assert_eq!(my_column.name, "my_column");
        assert_eq!(my_column.table, "");
        assert_eq!(my_column.schema, "");
    }

    #[test]
    fn test_column_ref_table() {
        let col = ColumnRef {
            name: "id".into(),
            table: "users".into(),
            schema: "public".into(),
        };
        let table = col.table();
        assert_eq!(table.name, "users");
        assert_eq!(table.schema, "public");
        assert_eq!(table.alias, "");
    }

    #[test]
    fn test_column_def_equality_and_hash() {
        #[derive(Entity)]
        struct TestTable {
            id: i32,
            name: String,
        }
        let cols = TestTable::columns();
        assert_ne!(cols[0], cols[1]);
        assert_eq!(cols[0], cols[0]);
        let mut set = HashSet::new();
        set.insert(&cols[0]);
        set.insert(&cols[1]);
        assert_eq!(set.len(), 2);
        set.insert(&cols[0]);
        assert_eq!(set.len(), 2);
    }
}
