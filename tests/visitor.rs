#[cfg(test)]
mod tests {
    use tank::{ColumnRef, Entity, Expression, GenericSqlWriter};

    #[derive(Entity)]
    struct Table {
        pub col_a: i64,
        #[tank(name = "second_column")]
        pub col_b: i128,
        pub str_column: String,
    }

    const WRITER: GenericSqlWriter = GenericSqlWriter {};
}
