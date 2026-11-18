#![allow(unused_imports)]
use std::sync::LazyLock;
use tank::{
    AsValue, Entity, Error, Executor, Result, Sql, Value, ValueOwned, anyhow, cols, expr,
    stream::{StreamExt, TryStreamExt},
};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Debug, PartialEq, Eq)]
// #region host-port
#[derive(Clone)]
pub struct HostPort {
    pub host: String,
    pub port: u16,
}

impl AsValue for HostPort {
    fn as_empty_value() -> ValueOwned {
        Value::Varchar(None)
    }

    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Varchar(Some(format!("{}:{}", self.host, self.port).into()))
    }

    fn try_from_value(value: impl AsValue) -> Result<Self>
    where
        Self: Sized,
    {
        // Always call try_as before checking the received value
        match value.as_value().try_as(&Value::Varchar(None)) {
            Ok(Value::Varchar(Some(v))) => {
                let context = || anyhow!("Failed to parse HostPort from value `{v}`");
                let (host, port) = v.split_once(':').ok_or_else(context)?;
                Ok(Self {
                    host: host.to_string(),
                    port: port
                        .parse::<u16>()
                        .map_err(|e| Error::new(e).context(context()))?,
                })
            }
            _ => Err(anyhow!(
                "Could not convert value into HostPort (expected Value::Varchar)",
            )),
        }
    }
}
// #endregion host-port

impl HostPort {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
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

pub async fn service(executor: &mut impl Executor) -> Result<()> {
    let _lock = MUTEX.lock().await;

    // Setup
    Service::drop_table(executor, true, false).await?;
    Service::create_table(executor, false, true).await?;

    // Query
    let mut api = Service {
        addr: HostPort::new("api.internal", 443),
        name: "api".into(),
        backup_addr: None,
    };
    api.save(executor).await?;
    let loaded = Service::find_one(executor, api.primary_key_expr())
        .await?
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
    api.save(executor).await?;
    let loaded = Service::find_one(executor, api.primary_key_expr())
        .await?
        .expect("Missing api after update");
    assert_eq!(
        loaded,
        Service {
            addr: HostPort::new("api.internal", 443),
            name: "api".into(),
            backup_addr: HostPort::new("api.internal", 8443).into(),
        }
    );

    let mut query = Service::prepare_find(executor, expr!(Service::name == ?), None).await?;
    query.bind("api")?;

    let api_by_addr = executor
        .fetch(query)
        .map_ok(Service::from_row)
        .map(Result::flatten)
        .try_collect::<Vec<_>>()
        .await?;
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
    Service::insert_one(executor, &api_canary).await?;

    let web = Service {
        addr: HostPort::new("web.internal", 80),
        name: "web".into(),
        backup_addr: Some(HostPort::new("web.internal", 8080)),
    };
    Service::insert_one(executor, &web).await?;
    #[cfg(not(feature = "disable-ordering"))]
    {
        let rows = executor
            .fetch(
                Sql()
                    .select(Service::columns())
                    .from(Service::table())
                    .where_expr(expr!(Service::name == "api"))
                    .order_by(cols!(Service::addr ASC))
                    .build(&executor.driver()),
            )
            .map_ok(Service::from_row)
            .map(Result::flatten)
            .try_collect::<Vec<_>>()
            .await?;

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
    Ok(())
}
