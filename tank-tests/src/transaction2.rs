use rust_decimal::Decimal;
use std::sync::LazyLock;
use tank::{Connection, Entity, FixedDecimal, Result, Transaction, expr};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, Debug, Clone)]
#[tank(primary_key = Self::id)]
struct Account {
    id: String,
    balance: FixedDecimal<12, 2>,
    active: bool,
    metadata: Option<String>,
    #[tank(column_type = (sqlite = "BLOB"))]
    payload: Option<Box<[u8]>>,
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

pub async fn transaction2(connection: &mut impl Connection) -> Result<()> {
    let _lock = MUTEX.lock().await;

    // Setup
    Account::drop_table(connection, true, false).await?;
    Account::create_table(connection, true, true).await?;
    Transfer::drop_table(connection, true, false).await?;
    Transfer::create_table(connection, true, true).await?;

    // Insert initial accounts
    let accounts = [
        Account {
            id: "A".into(),
            balance: Decimal::new(1000_00, 2).into(),
            active: true,
            metadata: Some("primary".into()),
            payload: None,
        },
        Account {
            id: "B".into(),
            balance: Decimal::new(500_00, 2).into(),
            active: true,
            metadata: None,
            payload: Some(vec![0x1, 0x2, 0x3].into()),
        },
        Account {
            id: "C".into(),
            balance: Decimal::new(0_00, 2).into(),
            active: true,
            metadata: None,
            payload: None,
        },
    ];
    Account::insert_many(connection, &accounts).await?;

    let mut tx = connection.begin().await?;

    // Transfer 200.00 A -> B
    let mut a = Account::find_one(&mut tx, expr!(Account::id == "A"))
        .await?
        .expect("Account A missing");
    let mut b = Account::find_one(&mut tx, expr!(Account::id == "B"))
        .await?
        .expect("Account B missing");

    let amount = Decimal::new(200_00, 2);
    a.balance.0 -= amount;
    b.balance.0 += amount;

    a.save(&mut tx).await?;
    b.save(&mut tx).await?;

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
    .await?;

    tx.commit().await?;

    let a_after = Account::find_one(connection, expr!(Account::id == "A"))
        .await?
        .expect("Account A missing after commit");
    let b_after = Account::find_one(connection, expr!(Account::id == "B"))
        .await?
        .expect("Account B missing after commit");
    let a_after_dec: Decimal = a_after.balance.into();
    let b_after_dec: Decimal = b_after.balance.into();
    assert_eq!(a_after_dec, Decimal::new(800_00, 2));
    assert_eq!(b_after_dec, Decimal::new(700_00, 2));

    // Transfer 300_00 B -> C and rollback
    let mut tx2 = connection.begin().await?;
    let mut b2 = Account::find_one(&mut tx2, expr!(Account::id == "B"))
        .await?
        .expect("Account B missing in tx2");
    let mut c2 = Account::find_one(&mut tx2, expr!(Account::id == "C"))
        .await?
        .expect("Account C missing in tx2");

    let t2_amount = Decimal::new(300_00, 2);
    b2.balance.0 -= t2_amount;
    c2.balance.0 += t2_amount;
    b2.save(&mut tx2).await?;
    c2.save(&mut tx2).await?;

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
    .await?;

    tx2.rollback().await?;

    let b_after_rb = Account::find_one(connection, expr!(Account::id == "B"))
        .await?
        .expect("Account B missing after rollback");
    let c_after_rb = Account::find_one(connection, expr!(Account::id == "C"))
        .await?
        .expect("Account C missing after rollback");
    let b_after_rb_dec: Decimal = b_after_rb.balance.into();
    let c_after_rb_dec: Decimal = c_after_rb.balance.into();
    assert_eq!(b_after_rb_dec, Decimal::new(700_00, 2));
    assert_eq!(c_after_rb_dec, Decimal::new(0_00, 2));

    // Delete account C and commit
    let mut tx3 = connection.begin().await?;
    let c_entity = Account::find_one(&mut tx3, expr!(Account::id == "C"))
        .await?
        .expect("Account C missing in tx3");
    c_entity.delete(&mut tx3).await?;
    tx3.commit().await?;
    let c_final = Account::find_one(connection, expr!(Account::id == "C")).await?;
    assert!(c_final.is_none(), "Account C should be deleted");
    Ok(())
}
