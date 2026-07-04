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

features:
  - icon: ⚡
    title: Async Firepower
    details: Execute queries asynchronously for maximum throughput and zero blocking overhead.
  - icon: ⚔️
    title: Raw Fire Control
    details: Deploy rapid native SQL precision strikes when high-caliber performance demands it.
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
