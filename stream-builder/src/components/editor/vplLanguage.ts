import type { languages } from 'monaco-editor'

export const VPL_LANGUAGE_ID = 'vpl'

export const vplLanguageConfig: languages.LanguageConfiguration = {
  comments: {
    lineComment: '#',
  },
  brackets: [
    ['(', ')'],
    ['[', ']'],
    ['{', '}'],
  ],
  autoClosingPairs: [
    { open: '(', close: ')' },
    { open: '[', close: ']' },
    { open: '{', close: '}' },
    { open: '"', close: '"' },
  ],
  surroundingPairs: [
    { open: '(', close: ')' },
    { open: '[', close: ']' },
    { open: '{', close: '}' },
    { open: '"', close: '"' },
  ],
}

export const vplTokensProvider: languages.IMonarchLanguage = {
  keywords: [
    'connector', 'event', 'stream', 'pattern', 'context',
    'const', 'let', 'var', 'fn', 'return', 'if', 'elif', 'else',
    'then', 'for', 'while', 'in', 'break', 'extends',
    'true', 'false', 'null', 'and', 'or', 'not',
    'as', 'where', 'within', 'partition', 'by',
    'merge', 'join', 'all', 'timer', 'SEQ', 'AND', 'OR', 'NOT',
  ],

  operators: [
    'from', 'to', 'emit', 'alert',
    'aggregate', 'window', 'partition_by', 'order_by',
    'having', 'select', 'distinct', 'limit',
    'map', 'filter', 'flat_map', 'collect',
    'forecast', 'score', 'enrich', 'trend_aggregate',
    'tap', 'log', 'print', 'watermark',
    'fork', 'any', 'first', 'process', 'on_error', 'concurrent',
    'context', 'on', 'allowed_lateness',
  ],

  builtinFunctions: [
    'count', 'sum', 'avg', 'min', 'max', 'first', 'last',
    'stddev', 'percentile', 'median', 'p50', 'p95', 'p99',
    'ema', 'count_distinct', 'len', 'str', 'now',
    'count_trends', 'count_events', 'avg_trends', 'sum_trends', 'max_trends',
    'concat',
  ],

  typeKeywords: [
    'str', 'float', 'int', 'bool', 'timestamp', 'duration',
    'list', 'map', 'any',
  ],

  tokenizer: {
    root: [
      // Comments
      [/#.*$/, 'comment'],

      // Strings
      [/"[^"]*"/, 'string'],

      // Numbers with units (durations)
      [/\d+[smhd]/, 'number.duration'],
      [/\d+\.\d+/, 'number.float'],
      [/\d+/, 'number'],

      // Operators
      [/->/, 'operator.sequence'],
      [/[=><!]+/, 'operator'],
      [/\.(?=\w)/, 'delimiter.dot'],

      // Identifiers
      [/[a-zA-Z_]\w*/, {
        cases: {
          '@keywords': 'keyword',
          '@operators': 'keyword.operator',
          '@builtinFunctions': 'support.function',
          '@typeKeywords': 'type',
          '@default': 'identifier',
        },
      }],

      // Delimiters
      [/[{}()\[\]]/, 'bracket'],
      [/[,:]/, 'delimiter'],
    ],
  },
}

export const vplTheme = {
  base: 'vs-dark' as const,
  inherit: true,
  rules: [
    { token: 'keyword', foreground: 'c084fc', fontStyle: 'bold' },
    { token: 'keyword.operator', foreground: '67e8f9' },
    { token: 'support.function', foreground: 'fbbf24' },
    { token: 'type', foreground: '34d399' },
    { token: 'string', foreground: 'a5d6a7' },
    { token: 'number', foreground: 'f9a825' },
    { token: 'number.float', foreground: 'f9a825' },
    { token: 'number.duration', foreground: 'ff8a65' },
    { token: 'comment', foreground: '6b7280', fontStyle: 'italic' },
    { token: 'operator.sequence', foreground: 'ef4444', fontStyle: 'bold' },
    { token: 'identifier', foreground: 'e2e8f0' },
    { token: 'delimiter.dot', foreground: '94a3b8' },
  ],
  colors: {
    'editor.background': '#0f172a',
    'editor.foreground': '#e2e8f0',
    'editorLineNumber.foreground': '#475569',
    'editorCursor.foreground': '#c084fc',
    'editor.selectionBackground': '#334155',
    'editor.lineHighlightBackground': '#1e293b',
  },
}
