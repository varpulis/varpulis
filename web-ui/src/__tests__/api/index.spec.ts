import { describe, it, expect, beforeEach } from 'vitest'
import { setApiKey, clearApiKey, getApiKey } from '@/api/index'

describe('API key management', () => {
  beforeEach(() => {
    sessionStorage.clear()
    localStorage.clear()
  })

  it('setApiKey stores key in sessionStorage', () => {
    setApiKey('test-key-abc')
    expect(sessionStorage.getItem('varpulis_api_key')).toBe('test-key-abc')
  })

  it('getApiKey retrieves key from sessionStorage', () => {
    sessionStorage.setItem('varpulis_api_key', 'my-key')
    expect(getApiKey()).toBe('my-key')
  })

  it('getApiKey returns null when no key set', () => {
    expect(getApiKey()).toBeNull()
  })

  it('clearApiKey removes key from sessionStorage', () => {
    setApiKey('key-to-remove')
    clearApiKey()
    expect(getApiKey()).toBeNull()
  })

  it('API key is NOT stored in localStorage', () => {
    setApiKey('secret')
    expect(localStorage.getItem('varpulis_api_key')).toBeNull()
  })
})
