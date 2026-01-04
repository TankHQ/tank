#![allow(unused_imports)]
use std::{
    collections::{BTreeMap, HashSet},
    sync::LazyLock,
};
use tank::{
    Entity, Executor, Passive, expr,
    stream::{StreamExt, TryStreamExt},
};
use time::macros::datetime;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Entity, Debug, Clone)]
// follower_count is PK to allow ordering for ScyllaDB / Cassandra
#[tank(schema = "testing", name = "user_profiles", primary_key = (id, follower_count))]
pub struct UserProfile {
    #[tank(name = "user_id")]
    pub id: Passive<Uuid>,
    #[tank(unique, column_type = (mysql = "VARCHAR(128)"))]
    pub username: String,
    #[tank(unique, column_type = (mysql = "VARCHAR(128)"))]
    pub email: String,
    pub full_name: Option<String>,
    #[tank(default = "0")]
    pub follower_count: u32,
    pub is_active: bool,
    pub last_login: Option<time::PrimitiveDateTime>,
    #[cfg(not(feature = "disable-maps"))]
    pub preferences: Option<BTreeMap<String, String>>,
}
static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub async fn users<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    // Cleanup
    let result = UserProfile::drop_table(executor, true, false).await;
    assert!(
        result.is_ok(),
        "Failed to UserProfile::drop_table: {:?}",
        result.unwrap_err()
    );

    // Setup
    let result = UserProfile::create_table(executor, false, true).await;
    assert!(
        result.is_ok(),
        "Failed to UserProfile::create_table: {:?}",
        result.unwrap_err()
    );

    // Insert
    let users_to_create = vec![
        UserProfile {
            id: Uuid::parse_str("a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1")
                .unwrap()
                .into(),
            username: "alice".into(),
            email: "alice@example.com".into(),
            full_name: Some("Alice Wonderland".into()),
            follower_count: 56,
            is_active: true,
            last_login: Some(datetime!(2025-07-15 10:00:00)),
            #[cfg(not(feature = "disable-maps"))]
            preferences: Some(BTreeMap::from_iter([("theme".into(), "dark".into())])),
        },
        UserProfile {
            id: Uuid::parse_str("b2b2b2b2-b2b2-b2b2-b2b2-b2b2b2b2b2b2")
                .unwrap()
                .into(),
            username: "bob".into(),
            email: "bob@example.com".into(),
            full_name: Some("Bob Builder".into()),
            follower_count: 99,
            is_active: false,
            last_login: None,
            #[cfg(not(feature = "disable-maps"))]
            preferences: Some(BTreeMap::from_iter([("theme".into(), "light".into())])),
        },
        UserProfile {
            id: Uuid::parse_str("c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3")
                .unwrap()
                .into(),
            username: "charlie".into(),
            email: "charlie@example.com".into(),
            full_name: None,
            follower_count: 5000,
            is_active: true,
            last_login: Some(datetime!(2025-07-16 11:30:00)),
            #[cfg(not(feature = "disable-maps"))]
            preferences: None,
        },
        UserProfile {
            id: Uuid::parse_str("d4d4d4d4-d4d4-d4d4-d4d4-d4d4d4d4d4d4")
                .unwrap()
                .into(),
            username: "dean".into(),
            email: "dean@example.com".into(),
            full_name: Some("Dean Martin".into()),
            follower_count: 15000,
            is_active: true,
            last_login: None,
            #[cfg(not(feature = "disable-maps"))]
            preferences: Some(BTreeMap::from_iter([(
                "notifications".into(),
                "off".into(),
            )])),
        },
        UserProfile {
            id: Uuid::parse_str("e5e5e5e5-e5e5-e5e5-e5e5-e5e5e5e5e5e5")
                .unwrap()
                .into(),
            username: "eve".into(),
            email: "eve@example.com".into(),
            full_name: Some("Eve".into()),
            follower_count: 1,
            is_active: false,
            last_login: Some(datetime!(2024-01-01 00:00:00)),
            #[cfg(not(feature = "disable-maps"))]
            preferences: None,
        },
    ];

    let result = UserProfile::insert_many(executor, users_to_create.iter()).await;
    assert!(
        result.is_ok(),
        "Failed to insert users: {:?}",
        result.unwrap_err()
    );
    if let Some(affected) = result.unwrap().rows_affected {
        assert_eq!(affected, 5);
    }

    // Find users with more than 1000 followers (should be 2: charlie, dean)
    let popular_users = UserProfile::find_many(executor, &expr!(follower_count > 1000), None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(popular_users.len(), 2);

    // Find active users (should be 3: alice, charlie, dean)
    let active_users = UserProfile::find_many(executor, &expr!(is_active == true), None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(active_users.len(), 3);
    let active_users = active_users
        .into_iter()
        .map(|u| u.username)
        .collect::<HashSet<_>>();
    assert_eq!(
        active_users,
        HashSet::from_iter(["alice".into(), "charlie".into(), "dean".into()])
    );

    // 4. Update a Bob
    let mut bob = UserProfile::find_one(executor, &expr!(username == "bob"))
        .await
        .expect("Expected query to succeed")
        .expect("Could not find bob ");
    bob.is_active = true;
    bob.full_name = Some("Robert Builder".into());
    bob.last_login = Some(datetime!(2025-07-17 20:00:00));
    let result = bob.save(executor).await;
    assert!(
        result.is_ok(),
        "Failed to save Bob: {:?}",
        result.unwrap_err()
    );
    let updated_bob = UserProfile::find_pk(executor, &bob.primary_key())
        .await
        .expect("Expected query to succeed")
        .expect("Could not find bob ");
    assert_eq!(updated_bob.is_active, true);
    assert_eq!(updated_bob.full_name, Some("Robert Builder".into()));
    assert!(updated_bob.last_login.is_some());

    // There must be 4 active users
    let active_users_after_update =
        UserProfile::find_many(executor, &expr!(is_active == true), None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
    assert_eq!(active_users_after_update.len(), 4);

    // Find eve user and delete it.
    let eve = UserProfile::find_one(executor, &expr!(username == "eve"))
        .await
        .expect("Expected query to succeed")
        .expect("Could not find eve ");
    let result = eve.delete(executor).await;
    assert!(
        result.is_ok(),
        "Failed to delete Eve: {:?}",
        result.unwrap_err()
    );
    let maybe_eve = UserProfile::find_pk(executor, &eve.primary_key())
        .await
        .expect("Expected query to succeed");
    assert!(maybe_eve.is_none(), "Eve should have been deleted");

    // There must be 5 total users
    let total_users = UserProfile::find_many(executor, &true, None).count().await;
    assert_eq!(total_users, 4, "There should be 4 users remaining");

    // Delete all users who never logged in (only Dean)
    let result = UserProfile::delete_many(executor, &expr!(last_login == NULL))
        .await
        .expect("Expected query to succeed");
    if let Some(affected) = result.rows_affected {
        assert_eq!(affected, 1, "Should have removed 1 rows");
    }

    // There must be 3 users left (alice, bob, charlie)
    let final_users = UserProfile::find_many(executor, &true, None)
        .try_collect::<Vec<_>>()
        .await
        .expect("Expected query to succeed");
    assert_eq!(final_users.len(), 3);
    let final_usernames = final_users
        .into_iter()
        .map(|u| u.username)
        .collect::<HashSet<_>>();
    assert_eq!(
        final_usernames,
        HashSet::from_iter(["alice".into(), "bob".into(), "charlie".into()])
    );
}
