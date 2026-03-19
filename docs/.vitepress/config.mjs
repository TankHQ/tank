import { defineConfig } from "vitepress"

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "Tank",
  description: "Table Abstraction and Navigation Kit",
  base: "/tank/",
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    search: {
      provider: 'local',
      options: {
        detailedView: true,
      },
    },

    nav: [
      { text: 'Home', link: '/' },
      { text: 'Docs', link: '/01-introduction' },
      { text: 'API', link: 'https://docs.rs/tank/' },
    ],

    sidebar: [
      {
        text: 'Docs',
        items: [
          { text: 'Introduction', link: '/01-introduction' },
          { text: 'Getting started', link: '/02-getting-started' },
          { text: 'Connection', link: '/03-connection' },
          { text: 'Capabilities', link: '/04-capabilities' },
          { text: 'Types', link: '/05-types' },
          { text: 'Entity definition', link: '/06-entity-definition' },
          { text: 'Simple operations', link: '/07-simple-operations' },
          { text: 'Advanced operations', link: '/08-advanced-operations' },
          { text: 'Raw SQL', link: '/09-raw-sql' },
          { text: 'Drivers', link: '/10-drivers' },
        ],
      },

    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/TankHQ/tank' },
    ],

    footer: {
      message: 'Released under the Apache-2.0 license.',
    }
  },

  head: [['link', { rel: 'icon', href: 'favicon.ico' }]],

  markdown: {
    config(md) {
    },
  },
})
