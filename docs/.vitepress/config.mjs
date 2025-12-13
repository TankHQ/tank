import { defineConfig } from 'vitepress'

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
      { text: 'Docs', link: '/1-introduction' },
      { text: 'API', link: 'https://docs.rs/tank/' },
    ],

    sidebar: [
      {
        text: 'Docs',
        items: [
          { text: 'Introduction', link: '/1-introduction' },
          { text: 'Getting started', link: '/2-getting-started' },
          { text: 'Connection', link: '/3-connection' },
          { text: 'Types', link: '/4-types' },
          { text: 'Entity definition', link: '/5-entity-definition' },
          { text: 'Simple operations', link: '/6-simple-operations' },
          { text: 'Advanced operations', link: '/7-advanced-operations' },
          { text: 'Raw SQL', link: '/8-raw-sql' },
          { text: 'Drivers', link: '/9-drivers' },
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

  markdown: {
    config(md) {
    },
  },
})
