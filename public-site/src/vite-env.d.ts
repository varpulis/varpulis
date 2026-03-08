/// <reference types="vite/client" />

declare const __APP_VERSION__: string

interface ImportMetaEnv {
  readonly VITE_API_KEY?: string
  readonly VITE_COORDINATOR_PORT?: string
  readonly VITE_APP_URL?: string
}
