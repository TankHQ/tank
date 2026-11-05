# Entity Definition
###### *Field Manual Section 6* - Unit Schematics

In Tank, an "Entity" is your frontline fighter: a Rust struct rigged with `#[derive(Entity)]` that maps to a database table and gives you functions to access and modify data. Tank handles the conversion between Rust values and database columns.

You need a live connection (see [*Field Manual Section 3 - Supply Lines*](03-connection.md#connect)) or a transaction to execute operations.

## Mission Briefing
Zero boilerplate. Define a struct, derive `Entity`. Field types map to driver column types.

## Entity
Start with a plain Rust struct and derive the `tank::Entity` trait. The fields can have any of the types supported (see [*Field Manual Section 5* - Payload Specs](05-types.md)).

```rust
use tank::Entity;
use time::PrimitiveDateTime;

#[derive(Entity)]
#[tank(schema = "ops", name = "missions", primary_key = (Self::code_name, Self::start_time))]
pub struct Mission {
    pub code_name: String,
    pub start_time: PrimitiveDateTime,
    #[tank(references = armory.weapons(serial_number))]
    pub primary_weapon: Option<i64>,
    pub objectives: Vec<String>,
    pub success_rate: f32,
    pub casualties: Option<u16>,
}
```

You now have a view of your table. Use a connection or transaction to run operations on it.

## Attributes
Tank's `#[tank(...)]` attributes configure tables and columns. DDL and constraint support depend on the driver. Declaring metadata does not guarantee backend enforcement.
- <Badge type="tip" text="struct" /><Badge type="tip" text="field" /> `name = "the_name"`: Table name on a struct or column name on a field. **Default**: snake_case struct name; field identifier for columns, with one leading underscore removed.
- <Badge type="tip" text="struct" /> `schema = "your_schema"`: Database schema. Default: none.
- <Badge type="tip" text="struct" /> `primary_key = "some_field"` or `primary_key = ("column_1", Self::column_2, ..)`: Table primary key.
- <Badge type="tip" text="field" /> `primary_key`: Marks field as part of primary key. Cannot be combined with struct-level `primary_key`.
- <Badge type="tip" text="struct" /> `unique = "some_field"` or `unique = ("column_1", Self::column_2, ..)`: Unique constraint.
- <Badge type="tip" text="field" /> `unique`: Field-level unique constraint.
- <Badge type="tip" text="field" /> `clustering_key`: Marks an existing primary-key field as a clustering key for ScyllaDB/Cassandra. Declare partition-key fields before clustering-key fields.
- <Badge type="tip" text="field" /> `column_type = (postgres = "TEXT", mysql = "VARCHAR(128)")`: Override column type in DDL (support depends on the driver).
- <Badge type="tip" text="field" /> `conversion_type = T`: Controls how a field is stored and loaded. `T` must implement `AsValue` and `From` conversions to and from the field type. Recognized field types may also need a matching `column_type` override when changing representation.
- <Badge type="tip" text="field" /> `ignore`: Excludes the field from storage and decoding. It retains the entity's `Default` value, or the field's default when the entity has no `Default` implementation.
- <Badge type="tip" text="field" /> `default = expression`: Column default expression, convertible to `DefaultValueType`.
- <Badge type="tip" text="field" /> `references = OtherEntity::column`: Foreign key reference.
- <Badge type="tip" text="field" /> `on_delete = no_action|restrict|cascade|set_null|set_default`: Action for foreign key when referenced row is deleted.
- <Badge type="tip" text="field" /> `on_update = no_action|restrict|cascade|set_null|set_default`: Action for foreign key when referenced row is updated.

### Examples
```rust
use tank::{AsValue, Entity};
use time::PrimitiveDateTime;
use uuid::{Uuid, uuid};

#[derive(Entity)]
#[tank(schema = "trading", name = "trade_execution", primary_key = ("trade_id", "execution_time"))]
pub struct Trade {
    #[tank(name = "trade_id")]
    pub trade: u64,
    #[tank(name = "order_id", default = uuid!("241d362d-797e-4769-b3f6-412440c8cf68").as_value())]
    pub order: Uuid,
    pub execution_time: PrimitiveDateTime,
}
```

*All units accounted for. Stand by.*
