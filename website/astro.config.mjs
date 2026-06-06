// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightThemeNova from 'starlight-theme-nova';

// https://astro.build/config
export default defineConfig({
  site: 'https://zendrite.pat-s.me',
  integrations: [
    starlight({
      customCss: ['./src/styles/custom.css'],
      plugins: [starlightThemeNova()],
      title: 'Zendrite',
      logo: {
        light: './src/assets/logo-light.svg',
        dark: './src/assets/logo-dark.svg',
        alt: 'Zendrite logo',
      },
      description: 'A second-generation Matrix homeserver written in Go',
      // social: [
      //   { icon: 'matrix', label: 'Matrix', href: 'https://matrix.to/#/#zendrite:matrix.org' },
      // ],
      sidebar: [
        {
          label: 'Getting Started',
          items: [
            { label: 'Introduction', slug: 'index' },
            { label: 'FAQ', slug: 'faq' },
            { label: 'MSC Support', slug: 'mscs' },
          ],
        },
        {
          label: 'Installation',
          items: [
            { label: 'Planning', slug: 'installation/planning' },
            { label: 'Domain Setup', slug: 'installation/domainname' },
            {
              label: 'Manual Installation',
              items: [
                { label: 'Building', slug: 'installation/manual/build' },
                { label: 'Database', slug: 'installation/manual/database' },
                { label: 'Signing Keys', slug: 'installation/manual/signingkey' },
                { label: 'Configuration', slug: 'installation/manual/configuration' },
                { label: 'Starting Zendrite', slug: 'installation/manual/starting' },
              ],
            },
            {
              label: 'Docker',
              items: [{ label: 'Docker Setup', slug: 'installation/docker/docker' }],
            },
            {
              label: 'Helm',
              items: [{ label: 'Helm Setup', slug: 'installation/helm/helm' }],
            },
          ],
        },
        {
          label: 'Administration',
          items: [
            { label: 'Creating Users', slug: 'administration/createusers' },
            { label: 'Registration', slug: 'administration/registration' },
            { label: 'Presence', slug: 'administration/presence' },
            { label: 'Admin API', slug: 'administration/adminapi' },
            { label: 'Auto-purging empty rooms', slug: 'administration/auto-purge-empty-rooms' },
            { label: 'Auto-forgetting rooms on leave', slug: 'administration/auto-forget-on-leave' },
            { label: 'Optimisation', slug: 'administration/optimisation' },
            { label: 'Migration from Dendrite', slug: 'administration/migration-from-dendrite' },
            { label: 'Backups', slug: 'administration/backups' },
            { label: 'Troubleshooting', slug: 'administration/troubleshooting' },
          ],
        },
        {
          label: 'Development',
          items: [
            { label: 'Architecture', slug: 'development/architecture' },
            { label: 'Contributing', slug: 'development/contributing' },
            { label: 'Profiling', slug: 'development/profiling' },
            { label: 'Coverage', slug: 'development/coverage' },
          ],
        },
      ],
      components: {
        // Override the `ThemeSelect` component from the Nova theme
        ThemeSelect: './src/components/ThemeSelect.astro',
        SocialIcons: './src/components/CodefloeIcon.astro',
      },
    }),
  ],
});
