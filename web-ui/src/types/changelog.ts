export type ChangeType = 'added' | 'fixed' | 'improved' | 'changed'

export interface ChangeItem {
  type: ChangeType
  text: string
}

export interface ChangelogEntry {
  version: string
  date: string
  title: string
  highlights?: string
  changes: ChangeItem[]
}
