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
    title: Async operations
    details: Your queries run async like artillery, load fast, return fire later.
  - icon: ⚔️
    title: Tactical simplicity
    details: Fire raw queries when you need precision strikes, use the abstractions for standard deployments.
  - icon: 🧩
    title: Battlefield adaptability
    details: Plug new backends in like changing magazines. Designed from day one to be extensible.
  - icon: 🎖️
    title: Rich type arsenal
    details: Map Rust types to SQL with automatic conversions.
---

<script setup>
  import TankJoke from "./components/TankJoke.vue"
</script>

<TankJoke />
