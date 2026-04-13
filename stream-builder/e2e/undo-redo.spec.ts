import { test, expect } from '@playwright/test'
import { toolbar } from './helpers'

test.describe('Undo / Redo', () => {
  test('undo button is disabled initially', async ({ page }) => {
    await page.goto('/')
    const undoBtn = page.getByTitle('Undo (Ctrl+Z)')
    await expect(undoBtn).toBeDisabled()
  })

  test('redo button is disabled initially', async ({ page }) => {
    await page.goto('/')
    const redoBtn = page.getByTitle('Redo (Ctrl+Shift+Z)')
    await expect(redoBtn).toBeDisabled()
  })

  test('undo after adding a stream restores empty state', async ({ page }) => {
    await page.goto('/')
    await toolbar(page).getByRole('button', { name: /Stream/ }).click()
    await expect(toolbar(page).getByText('1 stream', { exact: false })).toBeVisible()

    // Undo should be enabled
    const undoBtn = page.getByTitle('Undo (Ctrl+Z)')
    await expect(undoBtn).toBeEnabled()

    // Click undo
    await undoBtn.click()
    await expect(toolbar(page).getByText('0 streams')).toBeVisible()
    await expect(page.getByText('No streams yet')).toBeVisible()
  })

  test('redo after undo re-adds the stream', async ({ page }) => {
    await page.goto('/')
    await toolbar(page).getByRole('button', { name: /Stream/ }).click()
    await expect(toolbar(page).getByText('1 stream', { exact: false })).toBeVisible()

    // Undo
    await page.getByTitle('Undo (Ctrl+Z)').click()
    await expect(toolbar(page).getByText('0 streams')).toBeVisible()

    // Redo should be enabled
    const redoBtn = page.getByTitle('Redo (Ctrl+Shift+Z)')
    await expect(redoBtn).toBeEnabled()
    await redoBtn.click()

    await expect(toolbar(page).getByText('1 stream', { exact: false })).toBeVisible()
  })

  test('Ctrl+Z keyboard shortcut triggers undo', async ({ page }) => {
    await page.goto('/')
    await toolbar(page).getByRole('button', { name: /Stream/ }).click()
    await expect(toolbar(page).getByText('1 stream', { exact: false })).toBeVisible()

    await page.keyboard.press('Control+z')
    await expect(toolbar(page).getByText('0 streams')).toBeVisible()
  })

  test('Ctrl+Shift+Z keyboard shortcut triggers redo', async ({ page }) => {
    await page.goto('/')
    await toolbar(page).getByRole('button', { name: /Stream/ }).click()
    await page.keyboard.press('Control+z')
    await expect(toolbar(page).getByText('0 streams')).toBeVisible()

    await page.keyboard.press('Control+Shift+z')
    await expect(toolbar(page).getByText('1 stream', { exact: false })).toBeVisible()
  })

  test('multiple undos walk back through history', async ({ page }) => {
    await page.goto('/')
    const addBtn = toolbar(page).getByRole('button', { name: /Stream/ })

    await addBtn.click()
    await expect(toolbar(page).getByText('1 stream', { exact: false })).toBeVisible()
    await addBtn.click()
    await expect(toolbar(page).getByText('2 streams')).toBeVisible()
    await addBtn.click()
    await expect(toolbar(page).getByText('3 streams')).toBeVisible()

    // Undo 3 times
    await page.keyboard.press('Control+z')
    await expect(toolbar(page).getByText('2 streams')).toBeVisible()
    await page.keyboard.press('Control+z')
    await expect(toolbar(page).getByText('1 stream', { exact: false })).toBeVisible()
    await page.keyboard.press('Control+z')
    await expect(toolbar(page).getByText('0 streams')).toBeVisible()

    // Undo again should be no-op (already at start)
    const undoBtn = page.getByTitle('Undo (Ctrl+Z)')
    await expect(undoBtn).toBeDisabled()
  })
})
