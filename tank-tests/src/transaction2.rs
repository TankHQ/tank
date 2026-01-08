use std::sync::LazyLock;
use std::str::FromStr;
use rust_decimal::Decimal;
use tokio::sync::Mutex;
use tank::{Connection, DataSet, Entity, FixedDecimal, Value, cols, expr};
use tank::stream::{StreamExt, TryStreamExt};

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, Debug, Clone)]
#[tank(primary_key = Self::id)]
struct Account {
    id: String,
    balance: FixedDecimal<12, 2>,
    active: bool,
    metadata: Option<String>,
    payload: Option<Vec<u8>>,
}

#[derive(Entity, Debug, Clone)]
#[tank(primary_key = Self::id)]
struct Transfer {
    id: i64,
    from: String,
    to: String,
    amount: FixedDecimal<12, 2>,
    note: Option<String>,
}

pub async fn transaction2<C: Connection>(connection: &mut C) {
    let _lock = MUTEX.lock().await;

    // Setup
    Account::drop_table(connection, true, false)
        .await
        .expect("Failed to drop Account table");
    Account::create_table(connection, true, true)
        .await
        .expect("Failed to create Account table");
    Transfer::drop_table(connection, true, false)
        .await
        .expect("Failed to drop Transfer table");
    Transfer::create_table(connection, true, true)
        .await
        .expect("Failed to create Transfer table");

    // Insert initial accounts
    let accounts = [
        Account {
            id: "A".into(),
            balance: Decimal::new(1000_00).into(),
            active: true,
            metadata: Some("primary".into()),
            payload: None,
        },
        Account {
            id: "B".into(),
            balance: Decimal::new(500_00).into(),
            active: true,
            metadata: None,
            payload: Some(vec![1, 2, 3]),
        },
        Account {
            id: "C".into(),
            balance: Decimal::new(0_00).into(),
            active: true,
            metadata: None,
            payload: None,
        },
    ];
    Account::insert_many(connection, &accounts)
        .await
        .expect("Could not insert initial accounts");

    let mut tx = connection.begin().await.expect("Could not begin transaction");

    // Transfer 200.00 A -> B
    let mut a = Account::find_one(&mut tx, expr!(Account::id == "A"))
        .await
        .expect("Failed to query A")
        .expect("Account A missing");
    let mut b = Account::find_one(&mut tx, expr!(Account::id == "B"))
        .await
        .expect("Failed to query B")
        .expect("Account B missing");

    let amount = Decimal::new(200_00);
    a.balance.0 -= amount;
    b.balance.0 += amount;

    a.save(&mut tx).await.expect("Could not save A in tx");
    b.save(&mut tx).await.expect("Could not save B in tx");

    Transfer::insert_one(
        &mut tx,
        &Transfer {
            id: 1,
            from: "A".into(),
            to: "B".into(),
            amount: amount.into(),
            note: Some("A->B first transfer".into()),
        },
    )
    .await
    .expect("Could not insert transfer log");

    tx.commit().await.expect("Could not commit tx");

    let a_after = Account::find_one(connection, expr!(Account::id == "A"))
        .await
        .expect("Failed to read A after commit")
        .expect("Account A missing after commit");
    let b_after = Account::find_one(connection, expr!(Account::id == "B"))
        .await
        .expect("Failed to read B after commit")
        .expect("Account B missing after commit");
    let a_after_dec: Decimal = a_after.balance.into();
    let b_after_dec: Decimal = b_after.balance.into();
    assert_eq!(a_after_dec, Decimal::new(800_00));
    assert_eq!(b_after_dec, Decimal::new(700_00));

    // Transfer 300_00 B -> C and rollback
    let mut tx2 = connection.begin().await.expect("Could not begin second transaction");
    let mut b2 = Account::find_one(&mut tx2, expr!(Account::id == "B"))
        .await
        .expect("Failed to read B in tx2")
        .expect("Account B missing in tx2");
    let mut c2 = Account::find_one(&mut tx2, expr!(Account::id == "C"))
        .await
        .expect("Failed to read C in tx2")
        .expect("Account C missing in tx2");

    let t2_amount = Decimal::new(300_00);
    b2.balance.0 -= t2_amount;
    c2.balance.0 += t2_amount;
    b2.save(&mut tx2).await.expect("Could not save B in tx2");
    c2.save(&mut tx2).await.expect("Could not save C in tx2");

    Transfer::insert_one(
        &mut tx2,
        &Transfer {
            id: 2,
            from: "B".into(),
            to: "C".into(),
            amount: t2_amount.into(),
            note: Some("B->C rolled back".into()),
        },
    )
    .await
    .expect("Could not insert transfer log in tx2");

    tx2.rollback().await.expect("Could not rollback tx2");

    let b_after_rb = Account::find_one(connection, expr!(Account::id == "B"))
        .await
        .expect("Failed to read B after rollback")
        .expect("Account B missing after rollback");
    let c_after_rb = Account::find_one(connection, expr!(Account::id == "C"))
        .await
        .expect("Failed to read C after rollback")
        .expect("Account C missing after rollback");
    let b_after_rb_dec: Decimal = b_after_rb.balance.into();
    let c_after_rb_dec: Decimal = c_after_rb.balance.into();
    assert_eq!(b_after_rb_dec, Decimal::new(700_00));
    assert_eq!(c_after_rb_dec, Decimal::new(0_00));

    // Delete account C and commit
    let mut tx3 = connection.begin().await.expect("Could not begin third transaction");
    let c_entity = Account::find_one(&mut tx3, expr!(Account::id == "C"))
        .await
        .expect("Failed to read C in tx3")
        .expect("Account C missing in tx3");
    c_entity.delete(&mut tx3).await.expect("Could not delete C in tx3");
    tx3.commit().await.expect("Could not commit tx3");
    let c_final = Account::find_one(connection, expr!(Account::id == "C"))
        .await
        .expect("Failed to read C final");
    assert!(c_final.is_none(), "Account C should be deleted");
}
