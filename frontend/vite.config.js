import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/dashboard': 'http://127.0.0.1:8010',
      '/events': 'http://127.0.0.1:8010',
      '/simulator': 'http://127.0.0.1:8010',
      '/health': 'http://127.0.0.1:8010',
      '/metrics': 'http://127.0.0.1:8010',
      '/infra': 'http://127.0.0.1:8010',
      '/kafka': 'http://127.0.0.1:8010',
      '/war-room': 'http://127.0.0.1:8010',
      '/eval': 'http://127.0.0.1:8010',
      '/scenarios': 'http://127.0.0.1:8010',
    },
  },
  build: {
    outDir: 'dist',
  },
})
