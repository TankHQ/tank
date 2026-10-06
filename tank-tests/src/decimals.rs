#![allow(unused_imports)]
use tank::{Entity, Executor, FixedDecimal, Result, expr, stream::TryStreamExt};

#[derive(Entity, Debug, PartialEq)]
#[tank(schema = "testing", name = "high_scale_decimals")]
pub struct HighScaleDecimal {
    #[tank(primary_key)]
    pub id: i32,
    pub value: FixedDecimal<38, 30>,
}

pub async fn decimals(executor: &mut impl Executor) {
    HighScaleDecimal::drop_table(executor, true, false)
        .await
        .expect("Failed to drop the HighScaleDecimal table");
    HighScaleDecimal::create_table(executor, true, false)
        .await
        .expect("Failed to create the HighScaleDecimal table");

    let entity = HighScaleDecimal {
        id: 1,
        value: FixedDecimal::from("1.23".parse::<rust_decimal::Decimal>().unwrap()),
    };
    entity
        .save(executor)
        .await
        .expect("Failed to save the high-scale decimal");

    let loaded = HighScaleDecimal::find_one(executor, entity.primary_key_expr())
        .await
        .expect("Failed to query the high-scale decimal")
        .expect("The inserted high-scale decimal should be found");
    assert_eq!(
        loaded, entity,
        "The high-scale decimal changed on round-trip"
    );
    assert_eq!(
        loaded.value.0.normalize(),
        entity.value.0.normalize(),
        "The high-scale decimal value changed on round-trip"
    );
}
