# Getting Started
###### *Field Manual Section 2* - Deployment Orders

What follows is just a visit to the shooting range, not the full campaign. This minimal example shows Tank in action: connecting, defining a unit and executing basic maneuvers.

**Mission brief**: install Tank and a driver, define an entity, create the table, insert a few rows, then fire a query. For full tactical exercises including transactions, complex queries and multi-driver deployments, proceed to the [*Field Manual Section 3* - Supply Lines](03-connection.md).

> [!TIP]
> Tank exposes an async API. Run these examples under an async runtime like [Tokio](https://crates.io/crates/tokio) and `.await` your operations. Embedded drivers may still perform synchronous native work.

<!--@include: @/../README.md#getting-started-->

> [!NOTE]
> The `expr!` macro is explained in the [Field Manual Section 8 - Tactical Coordination](08-advanced-operations.md#expr).

*Rustaceans don't hide behind ORMs, they drive Tanks.*
