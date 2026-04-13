import { type Page } from '@playwright/test'

/** Toolbar: the top 40px bar */
export function toolbar(page: Page) {
  return page.locator('.h-10.border-b').first()
}

/** Sidebar: the 192px left panel */
export function sidebar(page: Page) {
  return page.locator('.w-48.border-r').first()
}

/** Assert stream count shown in the toolbar */
export function toolbarStreamCount(page: Page, text: string) {
  return toolbar(page).locator('.tabular-nums').filter({ hasText: text })
}
