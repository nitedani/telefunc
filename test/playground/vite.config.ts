import telefunc from 'telefunc/vite'
import react from '@vitejs/plugin-react'
import vike from 'vike/plugin'
import type { UserConfig } from 'vite'

export default {
<<<<<<< Updated upstream
  plugins: [react(), vike(), telefunc()],
  // @ts-expect-error
  vitePluginServerEntry: {
    disableAutoImport: true,
=======
  server: {
    host: true,
    hmr: {
      port: 24679,
    },
>>>>>>> Stashed changes
  },
  build: {
    outDir: `${__dirname}/../../test/playground/dist/nested`,
  },
} satisfies UserConfig
