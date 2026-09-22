---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "TANK"
  text: "Table Abstraction & Navigation Kit"
  tagline: The Rust data layer
  image:
    src: logo.png
    alt: Tank logo
  actions:
    - theme: brand
      text: Getting started
      link: /02-getting-started
    - theme: alt
      text: View on crates.io
      link: https://crates.io/crates/tank

features:
  - icon: ⚡
    title: Async Firepower
    details: Build on non-blocking database operations designed for async Rust applications.
  - icon: ⚔️
    title: Explicit Fire Control
    details: Use typed expressions and joins, then deploy raw SQL when the abstraction is not enough.
  - icon: 🧩
    title: Adaptable Chassis
    details: Swap database backends seamlessly like changing magazines mid-battle without friction.
  - icon: 🎖️
    title: Rich type arsenal
    details: Convert complex Rust types to database equivalents automatically and safely.
---

<script setup>
  import TankJoke from "./components/TankJoke.vue"
</script>

<TankJoke />
