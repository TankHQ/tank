use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
#[cfg(not(feature = "disable-intervals"))]
use tank::Interval;
use tank::{Entity, Executor, Result, expr};
use time::{PrimitiveDateTime, macros::datetime};

#[derive(Entity, Debug, PartialEq)]
pub struct OtherTypes {
    #[tank(primary_key)]
    id: i32,
    #[cfg(not(feature = "disable-intervals"))]
    duration_value: Option<Interval>,
    timestamp_value: Option<PrimitiveDateTime>,
    inet_value: Option<IpAddr>,
    #[cfg(not(feature = "disable-large-integers"))]
    varint_value: Option<i128>,
    #[cfg(not(feature = "disable-large-integers"))]
    varint_unsigned_value: Option<u128>,
}

pub async fn other_types(executor: &mut impl Executor) -> Result<()> {
    OtherTypes::drop_table(executor, true, false).await?;
    OtherTypes::create_table(executor, true, true).await?;

    let ipv4 = OtherTypes {
        id: 1,
        #[cfg(not(feature = "disable-intervals"))]
        duration_value: Some(
            Interval::from_mins(5) + Interval::from_secs(24) + Interval::from_millis(33),
        ),
        timestamp_value: Some(datetime!(2023-05-06 07:08:09)),
        inet_value: Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))),
        #[cfg(not(feature = "disable-large-integers"))]
        varint_value: Some(i128::MIN),
        #[cfg(not(feature = "disable-large-integers"))]
        varint_unsigned_value: Some(0),
    };
    ipv4.save(executor).await?;
    assert_eq!(
        OtherTypes::find_one(executor, expr!(OtherTypes::id == 1)).await?,
        Some(ipv4)
    );

    let ipv6 = OtherTypes {
        id: 2,
        #[cfg(not(feature = "disable-intervals"))]
        duration_value: Some(-Interval::from_days(11)),
        timestamp_value: Some(datetime!(1970-01-01 00:00:00)),
        inet_value: Some(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1))),
        #[cfg(not(feature = "disable-large-integers"))]
        varint_value: Some(i128::MAX),
        #[cfg(not(feature = "disable-large-integers"))]
        varint_unsigned_value: Some(u128::MAX),
    };
    ipv6.save(executor).await?;
    assert_eq!(
        OtherTypes::find_one(executor, expr!(OtherTypes::id == 2)).await?,
        Some(ipv6)
    );

    let nulls = OtherTypes {
        id: 3,
        #[cfg(not(feature = "disable-intervals"))]
        duration_value: None,
        timestamp_value: None,
        inet_value: None,
        #[cfg(not(feature = "disable-large-integers"))]
        varint_value: None,
        #[cfg(not(feature = "disable-large-integers"))]
        varint_unsigned_value: None,
    };
    nulls.save(executor).await?;
    assert_eq!(
        OtherTypes::find_one(executor, expr!(OtherTypes::id == 3)).await?,
        Some(nulls)
    );

    Ok(())
}
