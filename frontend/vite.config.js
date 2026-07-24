import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/health': 'http://127.0.0.1:8010',
      '/simulate': 'http://127.0.0.1:8010',
      '/incidents': 'http://127.0.0.1:8010',
      '/predictions': 'http://127.0.0.1:8010',
      '/ml': 'http://127.0.0.1:8010',
      '/metrics': 'http://127.0.0.1:8010',
    },
  },
  build: {
    outDir: 'dist',
  },
})
