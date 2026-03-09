import { onUnmounted } from 'vue'

function setMeta(name: string, content: string, attr = 'name') {
  let el = document.querySelector(`meta[${attr}="${name}"]`) as HTMLMetaElement | null
  if (!el) {
    el = document.createElement('meta')
    el.setAttribute(attr, name)
    document.head.appendChild(el)
  }
  el.setAttribute('content', content)
}

export function useSeoMeta(options: {
  title: string
  description: string
  ogType?: string
}) {
  const prev = document.title
  document.title = `${options.title} | Varpulis`
  setMeta('description', options.description)
  setMeta('og:title', options.title, 'property')
  setMeta('og:description', options.description, 'property')
  setMeta('og:type', options.ogType || 'website', 'property')
  setMeta('og:site_name', 'Varpulis', 'property')
  setMeta('twitter:card', 'summary_large_image')
  setMeta('twitter:title', options.title)
  setMeta('twitter:description', options.description)

  onUnmounted(() => {
    document.title = prev
  })
}
