use std::{
    borrow::Cow,
    sync::{
        LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tank::{
    AsValue, Entity, Error, Executor, QueryBuilder, Result, Value, current_timestamp_ms, expr,
    join, stream::StreamExt,
};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Method {
    GET,
    POST,
    PUT,
    DELETE,
}
impl AsValue for Method {
    fn as_empty_value() -> Value {
        Value::Varchar(None)
    }
    fn as_value(self) -> Value {
        Value::Varchar(Some(
            match self {
                Method::GET => "get",
                Method::POST => "post",
                Method::PUT => "put",
                Method::DELETE => "delete",
            }
            .into(),
        ))
    }
    fn try_from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        if let Value::Varchar(Some(v)) = value.try_as(&String::as_empty_value())? {
            match &*v {
                "get" => return Ok(Method::GET),
                "post" => return Ok(Method::POST),
                "put" => return Ok(Method::PUT),
                "delete" => return Ok(Method::DELETE),
                _ => {
                    return Err(Error::msg(format!(
                        "Unexpected value `{v}` for Method enum"
                    )));
                }
            }
        }
        Err(Error::msg("Unexpected value for Method enum"))
    }
}

#[derive(Default, Entity, PartialEq, Eq)]
#[tank(schema = "api")]
struct RequestLimit {
    #[tank(primary_key)]
    pub id: i32,
    pub target_pattern: Cow<'static, str>,
    pub requests: i32,
    // If set it applies only to the requests with that method, otherwise it affets all methods
    pub method: Option<Method>,
    // If set, it means maximum request in unit of time, otherwise means maximum concurrent requests
    pub interval_ms: Option<i32>,
}
impl RequestLimit {
    pub fn new(
        target_pattern: &'static str,
        requests: i32,
        method: Option<Method>,
        interval_ms: Option<i32>,
    ) -> Self {
        let id = GLOBAL_COUNTER.fetch_add(1, Ordering::Relaxed) as _;
        Self {
            id,
            target_pattern: target_pattern.into(),
            requests,
            method,
            interval_ms,
        }
    }
}

#[derive(Entity, PartialEq, Eq)]
#[tank(schema = "api")]
pub struct Request {
    #[tank(primary_key)]
    pub id: i64,
    pub target: String,
    pub method: Option<Method>,
    pub beign_timestamp_ms: i64,
    pub end_timestamp_ms: Option<i64>,
}

static GLOBAL_COUNTER: AtomicUsize = AtomicUsize::new(0);

impl Request {
    pub fn new(target: String, method: Option<Method>) -> Self {
        let id = GLOBAL_COUNTER.fetch_add(1, Ordering::Relaxed) as _;
        Self {
            id,
            target,
            method,
            beign_timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as _,
            end_timestamp_ms: None,
        }
    }
    pub fn end(&mut self) {
        self.end_timestamp_ms = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as _,
        );
    }
}

pub async fn requests<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock();

    // Setup
    RequestLimit::drop_table(executor, true, false)
        .await
        .expect("Could not drop the RequestLimit table");
    Request::drop_table(executor, true, false)
        .await
        .expect("Could not drop the Request table");

    RequestLimit::create_table(executor, false, true)
        .await
        .expect("Could not create the RequestLimit table");
    Request::create_table(executor, false, true)
        .await
        .expect("Could not create the Request table");

    // Request limits
    RequestLimit::insert_many(
        executor,
        &[
            // [1]: Max 3 concurrent requests
            RequestLimit::new("v1/%", 3, None, None),
            // [2]:  Max 5 data concurrent requests
            RequestLimit::new("v1/server/data/%", 5, None, None),
            // [3]:  Max 2 user concurrent put request
            RequestLimit::new("v1/server/user/%", 2, Method::PUT.into(), None),
            // [4]:  Max 1 user concurrent delete request
            RequestLimit::new("v1/server/user/%", 1, Method::DELETE.into(), None),
            // [5]:  Max 5 requests
            RequestLimit::new("v2/%", 5, None, 60_000.into()),
        ],
    )
    .await
    .expect("Could not insert the limits");
    let limits = RequestLimit::find_many(executor, true, None)
        .map(|v| v.expect("Found error"))
        .count()
        .await;
    assert_eq!(limits, 5);

    #[cfg(not(feature = "disable-joins"))]
    {
        let mut violated_limits = executor
        .prepare(
            QueryBuilder::new()
                .select([
                    RequestLimit::target_pattern,
                    RequestLimit::requests,
                    RequestLimit::method,
                    RequestLimit::interval_ms,
                ])
                .from(join!(RequestLimit CROSS JOIN Request))
                .where_expr(expr!(
                    ? == RequestLimit::target_pattern as LIKE
                        && Request::target == RequestLimit::target_pattern as LIKE
                        && (RequestLimit::method == NULL
                            || RequestLimit::method == Request::method)
                        && (RequestLimit::interval_ms == NULL && Request::end_timestamp_ms == NULL
                            || RequestLimit::interval_ms != NULL
                                && Request::end_timestamp_ms
                                    >= current_timestamp_ms!() - RequestLimit::interval_ms)
                ))
                .group_by([
                    RequestLimit::target_pattern,
                    RequestLimit::requests,
                    RequestLimit::method,
                    RequestLimit::interval_ms,
                ])
                .having(expr!(COUNT(Request::id) >= RequestLimit::requests))
                .build(&executor.driver()),
        )
        .await
        .expect("Failed to prepare the limit query");

        let mut r1 = Request::new("v1/server/user/new/1".into(), Method::PUT.into());
        let mut r2 = Request::new("v1/server/user/new/2".into(), Method::PUT.into());
        let r3 = Request::new("v1/server/user/new/3".into(), Method::PUT.into());
        let r4 = Request::new("v1/server/articles/new/4".into(), Method::PUT.into());
        let r5 = Request::new("v1/server/user/new/5".into(), Method::PUT.into());

        violated_limits.bind(r1.target.clone()).unwrap();
        assert_eq!(executor.fetch(&mut violated_limits).count().await, 0);
        r1.save(executor).await.expect("Failed to save r1");

        violated_limits.bind(r2.target.clone()).unwrap();
        assert_eq!(executor.fetch(&mut violated_limits).count().await, 0);
        r2.save(executor).await.expect("Failed to save r2");

        violated_limits.bind(r3.target.clone()).unwrap();
        assert_eq!(executor.fetch(&mut violated_limits).count().await, 1); // Violates [3]

        // Request 4 fits because it doesn't refer to /user
        violated_limits.bind(r4.target.clone()).unwrap();
        assert_eq!(executor.fetch(&mut violated_limits).count().await, 0);
        r4.save(executor).await.expect("Failed to save r4");

        violated_limits.bind(r5.target.clone()).unwrap();
        assert_eq!(executor.fetch(&mut violated_limits).count().await, 2); // Violates [1], [3]

        r1.end();
        r1.save(executor).await.expect("Could not terminate r1");

        violated_limits.bind(r3.target.clone()).unwrap();
        assert_eq!(executor.fetch(&mut violated_limits).count().await, 0);
        r3.save(executor).await.expect("Failed to save r3");

        // 3 Running requests

        let mut data_reqs = vec![];
        for i in 0..5 {
            let req = Request::new(format!("v1/server/data/item/{}", i), None);
            req.save(executor)
                .await
                .expect("Failed to save data request");
            data_reqs.push(req);
        }

        // 8 Running requests

        violated_limits
            .bind("v1/server/data/item/999".to_string())
            .unwrap();
        assert_eq!(executor.fetch(&mut violated_limits).count().await, 2); // Violates [1], [2]

        for i in 0..4 {
            data_reqs[i].end();
            data_reqs[i]
                .save(executor)
                .await
                .expect(&format!("Failed to save data_reqs[{i}]"));
        }

        // 4 Running requests

        violated_limits
            .bind("v1/server/data/item/999".to_string())
            .unwrap();
        assert_eq!(executor.fetch(&mut violated_limits).count().await, 1); // Violates [1] still

        r2.end();
        r2.save(executor).await.expect("Could not terminate r2");
        data_reqs[4].end();
        data_reqs[4]
            .save(executor)
            .await
            .expect("Could not terminate data_reqs[4]");

        violated_limits
            .bind("v1/server/data/item/999".to_string())
            .unwrap();
        assert_eq!(executor.fetch(&mut violated_limits).count().await, 0);
    }
}
