export interface DocPage {
  slug: string
  title: string
  category: string
  order: number
}

export interface DocCategory {
  name: string
  icon: string
  pages: DocPage[]
}
