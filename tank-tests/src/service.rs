#![allow(unused_imports)]
use std::sync::LazyLock;
use tank::{
    AsValue, Entity, Error, Executor, QueryBuilder, Result, Value, cols, expr,
    stream::{StreamExt, TryStreamExt},
};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostPort {
    pub host: String,
    pub port: u16,
}

impl HostPort {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }
}

impl AsValue for HostPort {
    fn as_empty_value() -> Value {
        Value::Varchar(None)
    }

    fn as_value(self) -> Value {
        Value::Varchar(Some(format!("{}:{}", self.host, self.port).into()))
    }

    fn try_from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        if let Value::Varchar(Some(v), ..) = value.try_as(&Value::Varchar(None))? {
            let (host, port) = v
                .split_once(':')
                .ok_or_else(|| Error::msg(format!("Invalid HostPort `{v}`")))?;

            return Ok(Self {
                host: host.to_string(),
                port: port
                    .parse::<u16>()
                    .map_err(|_| Error::msg(format!("Invalid port in HostPort `{v}`")))?,
            });
        }
        Err(Error::msg("Unexpected value for HostPort"))
    }
}

#[derive(Entity, Clone, Debug, PartialEq, Eq)]
#[tank(schema = "ops")]
#[tank(primary_key = (name, addr))]
struct Service {
    name: String,
    #[tank(clustering_key)]
    addr: HostPort,
    backup_addr: Option<HostPort>,
}

pub async fn service(executor: &mut impl Executor) {
    let _lock = MUTEX.lock().await;

    // Setup
    Service::drop_table(executor, true, false)
        .await
        .expect("Failed to drop Service");
    Service::create_table(executor, false, true)
        .await
        .expect("Failed to create Service");

    // Query
    let mut api = Service {
        addr: HostPort::new("api.internal", 443),
        name: "api".into(),
        backup_addr: None,
    };
    api.save(executor).await.expect("Failed to save api");
    let loaded = Service::find_one(executor, api.primary_key_expr())
        .await
        .expect("Failed to load api")
        .expect("Missing api");
    assert_eq!(
        loaded,
        Service {
            addr: HostPort::new("api.internal", 443),
            name: "api".into(),
            backup_addr: None,
        }
    );

    api.backup_addr = Some(HostPort::new("api.internal", 8443));
    api.save(executor)
        .await
        .expect("Failed to update api backup");
    let loaded = Service::find_one(executor, api.primary_key_expr())
        .await
        .expect("Failed to reload api")
        .expect("Missing api after update");
    assert_eq!(
        loaded,
        Service {
            addr: HostPort::new("api.internal", 443),
            name: "api".into(),
            backup_addr: HostPort::new("api.internal", 8443).into(),
        }
    );

    let mut query = Service::prepare_find(executor, expr!(Service::name == ?), None)
        .await
        .expect("Failed to prepare query by name");
    query.bind("api").expect("Failed to bind name parameter");

    let api_by_addr = executor
        .fetch(query)
        .map_ok(Service::from_row)
        .map(Result::flatten)
        .try_collect::<Vec<_>>()
        .await
        .expect("Failed to query by addr");
    assert_eq!(
        api_by_addr,
        [Service {
            addr: HostPort::new("api.internal", 443),
            name: "api".into(),
            backup_addr: HostPort::new("api.internal", 8443).into(),
        }]
    );

    let api_canary = Service {
        addr: HostPort::new("api.internal", 8444),
        name: "api".into(),
        backup_addr: None,
    };
    Service::insert_one(executor, &api_canary)
        .await
        .expect("Failed to insert api canary");

    let web = Service {
        addr: HostPort::new("web.internal", 80),
        name: "web".into(),
        backup_addr: Some(HostPort::new("web.internal", 8080)),
    };
    Service::insert_one(executor, &web)
        .await
        .expect("Failed to insert web");
    #[cfg(not(feature = "disable-ordering"))]
    {
        let rows = executor
            .fetch(
                QueryBuilder::new()
                    .select(Service::columns())
                    .from(Service::table())
                    .where_expr(expr!(Service::name == "api"))
                    .order_by(cols!(Service::addr ASC))
                    .build(&executor.driver()),
            )
            .map_ok(Service::from_row)
            .map(Result::flatten)
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to select ordered services");

        assert_eq!(
            rows,
            [
                Service {
                    addr: HostPort::new("api.internal", 443),
                    name: "api".into(),
                    backup_addr: HostPort::new("api.internal", 8443).into(),
                },
                Service {
                    addr: HostPort::new("api.internal", 8444),
                    name: "api".into(),
                    backup_addr: None,
                },
            ]
        );
    }
}
