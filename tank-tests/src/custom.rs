use crate::silent_logs;
use std::{
    num::{FpCategory, NonZeroI32},
    sync::LazyLock,
};
use tank::{AsValue, Entity, Error, Executor, Result, Value, expr};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, PartialEq, Debug)]
pub struct CustomValues {
    #[tank(primary_key)]
    pk: NonZeroI32,
    #[tank(conversion_type = FpCategoryWrap)]
    category: FpCategory,
}
pub struct FpCategoryWrap(pub FpCategory);
impl AsValue for FpCategoryWrap {
    fn as_empty_value() -> Value {
        Value::Varchar(None)
    }
    fn as_value(self) -> Value {
        format!("{:?}", self.0).as_value()
    }

    fn try_from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        let make_error = || Error::msg(format!("Could not conver `{value:?}` to FpCategory"));
        let Value::Varchar(Some(value), ..) = &value else {
            return Err(make_error());
        };
        Ok(match value.as_ref() {
            "Nan" => FpCategory::Nan,
            "Infinite" => FpCategory::Infinite,
            "Zero" => FpCategory::Zero,
            "Subnormal" => FpCategory::Subnormal,
            "Normal" => FpCategory::Normal,
            _ => return Err(make_error()),
        }
        .into())
    }
}
impl From<FpCategory> for FpCategoryWrap {
    fn from(value: FpCategory) -> Self {
        Self(value)
    }
}
impl From<FpCategoryWrap> for FpCategory {
    fn from(value: FpCategoryWrap) -> Self {
        value.0
    }
}

pub async fn custom(executor: &mut impl Executor) {
    let _lock = MUTEX.lock().await;

    // Setup
    silent_logs! {
        // Silent logs for Valkey/Redis
        CustomValues::drop_table(executor, true, false)
            .await
            .expect("Failed to drop SimpleNullFields table");
    }
    CustomValues::create_table(executor, true, true)
        .await
        .expect("Failed to create SimpleNullFields table");

    // Query
    CustomValues::insert_many(
        executor,
        &[
            CustomValues {
                pk: 50.try_into().unwrap(),
                category: FpCategory::Subnormal,
            },
            CustomValues {
                pk: 51.try_into().unwrap(),
                category: FpCategory::Infinite,
            },
        ],
    )
    .await
    .expect("Failed to insert values");
    CustomValues {
        pk: 52.try_into().unwrap(),
        category: FpCategory::Nan,
    }
    .save(executor)
    .await
    .expect("Failed to save");
    let value = CustomValues::find_one(executor, expr!(CustomValues::pk == 52))
        .await
        .expect("Failed to find");
    assert_eq!(
        value,
        Some(CustomValues {
            pk: 52.try_into().unwrap(),
            category: FpCategory::Nan
        })
    );
}
