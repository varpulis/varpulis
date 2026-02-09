// VPL (Varpulis Pipeline Language) Monaco Language Definition

export function registerVplLanguage(getConnectorNames?: () => string[]): void {
  // Check if monaco is available
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const monaco = (window as any).monaco
  if (!monaco) {
    console.warn('Monaco not loaded yet, skipping VPL language registration')
    return
  }

  // Check if already registered
  const languages = monaco.languages.getLanguages()
  if (languages.some((lang: { id: string }) => lang.id === 'vpl')) {
    return
  }

  // Register the language
  monaco.languages.register({ id: 'vpl' })

  // Define tokens
  monaco.languages.setMonarchTokensProvider('vpl', {
    keywords: [
      'stream', 'event', 'connector', 'context', 'fn', 'let', 'var', 'const',
      'if', 'else', 'elif', 'for', 'while', 'return', 'pattern', 'config',
      'from', 'where', 'select', 'window', 'aggregate', 'emit', 'to',
      'join', 'merge', 'partition_by', 'order_by', 'limit', 'distinct',
      'true', 'false', 'null', 'and', 'or', 'not', 'in', 'is',
      'SEQ', 'NOT', 'AND', 'OR', 'within', 'as', 'import', 'export'
    ],
    typeKeywords: [
      'int', 'float', 'bool', 'str', 'timestamp', 'duration', 'Stream',
      'Event', 'Context', 'Option', 'Result', 'List', 'Map', 'Set'
    ],
    operators: [
      '+', '-', '*', '/', '%', '**',
      '==', '!=', '<', '<=', '>', '>=',
      '=', ':=', '=>', '->', '.', '?.', '..', '..=',
      '&&', '||', '!', '&', '|', '^', '~',
      '<<', '>>'
    ],
    symbols: /[=><!~?:&|+\-*\/\^%]+/,
    escapes: /\\(?:[abfnrtv\\"']|x[0-9A-Fa-f]{1,4}|u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8})/,
    digits: /\d+(_+\d+)*/,
    octaldigits: /[0-7]+(_+[0-7]+)*/,
    binarydigits: /[0-1]+(_+[0-1]+)*/,
    hexdigits: /[[0-9a-fA-F]+(_+[0-9a-fA-F]+)*/,

    tokenizer: {
      root: [
        // Identifiers and keywords
        [/[a-z_$][\w$]*/, {
          cases: {
            '@keywords': 'keyword',
            '@typeKeywords': 'type',
            '@default': 'identifier'
          }
        }],
        [/[A-Z][\w$]*/, 'type.identifier'],

        // Whitespace
        { include: '@whitespace' },

        // Delimiters and operators
        [/[{}()\[\]]/, '@brackets'],
        [/[<>](?!@symbols)/, '@brackets'],
        [/@symbols/, {
          cases: {
            '@operators': 'operator',
            '@default': ''
          }
        }],

        // Numbers
        [/(@digits)[eE]([\-+]?(@digits))?/, 'number.float'],
        [/(@digits)\.(@digits)([eE][\-+]?(@digits))?/, 'number.float'],
        [/0[xX](@hexdigits)/, 'number.hex'],
        [/0[oO]?(@octaldigits)/, 'number.octal'],
        [/0[bB](@binarydigits)/, 'number.binary'],
        [/(@digits)/, 'number'],

        // Duration literals
        [/\d+[smhd]/, 'number.duration'],
        [/\d+ms/, 'number.duration'],

        // Delimiter
        [/[;,.]/, 'delimiter'],

        // Strings
        [/"([^"\\]|\\.)*$/, 'string.invalid'],
        [/"/, { token: 'string.quote', bracket: '@open', next: '@string' }],
        [/'([^'\\]|\\.)*$/, 'string.invalid'],
        [/'/, { token: 'string.quote', bracket: '@open', next: '@stringSingle' }],
      ],

      comment: [
        [/[^\/*]+/, 'comment'],
        [/\/\*/, 'comment', '@push'],
        ["\\*/", 'comment', '@pop'],
        [/[\/*]/, 'comment']
      ],

      string: [
        [/[^\\"]+/, 'string'],
        [/@escapes/, 'string.escape'],
        [/\\./, 'string.escape.invalid'],
        [/"/, { token: 'string.quote', bracket: '@close', next: '@pop' }]
      ],

      stringSingle: [
        [/[^\\']+/, 'string'],
        [/@escapes/, 'string.escape'],
        [/\\./, 'string.escape.invalid'],
        [/'/, { token: 'string.quote', bracket: '@close', next: '@pop' }]
      ],

      whitespace: [
        [/[ \t\r\n]+/, 'white'],
        [/\/\*/, 'comment', '@comment'],
        [/\/\/.*$/, 'comment'],
        [/#.*$/, 'comment'],
      ],
    },
  })

  // Define completion items
  monaco.languages.registerCompletionItemProvider('vpl', {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    provideCompletionItems: (model: any, position: any) => {
      // Context-aware: detect .from( or .to( and suggest cluster connectors
      const textUntilPosition = model.getValueInRange({
        startLineNumber: position.lineNumber,
        startColumn: 1,
        endLineNumber: position.lineNumber,
        endColumn: position.column,
      })

      if (/\.(from|to)\(\s*$/.test(textUntilPosition) && getConnectorNames) {
        const names = getConnectorNames()
        const connectorSuggestions = names.map((name: string) => ({
          label: name,
          kind: monaco.languages.CompletionItemKind.Reference,
          insertText: name,
          detail: 'Cluster connector',
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          range: undefined as any,
        }))
        return { suggestions: connectorSuggestions }
      }

      const suggestions = [
        // Keywords
        { label: 'stream', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'stream ${1:name} {\n\t$0\n}', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet },
        { label: 'event', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'event ${1:Name} {\n\t${2:field}: ${3:type}$0\n}', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet },
        { label: 'pattern', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'pattern ${1:name} = ${2:SEQ}(\n\t$0\n);', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet },
        { label: 'context', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'context ${1:name} {\n\t$0\n}', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet },
        { label: 'fn', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'fn ${1:name}(${2:params}) -> ${3:type} {\n\t$0\n}', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet },
        { label: 'connector', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'connector ${1:name} {\n\ttype: "${2:kafka}",\n\t$0\n}', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet },

        // Pattern operators
        { label: 'SEQ', kind: monaco.languages.CompletionItemKind.Function, insertText: 'SEQ(\n\t${1:e1}: ${2:EventType},\n\t${3:e2}: ${4:EventType}\n) where $0', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, documentation: 'Sequence pattern - matches events in order' },
        { label: 'AND', kind: monaco.languages.CompletionItemKind.Function, insertText: 'AND(\n\t${1:pattern1},\n\t${2:pattern2}\n)', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, documentation: 'Conjunction - all patterns must match' },
        { label: 'OR', kind: monaco.languages.CompletionItemKind.Function, insertText: 'OR(\n\t${1:pattern1},\n\t${2:pattern2}\n)', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, documentation: 'Disjunction - any pattern can match' },
        { label: 'NOT', kind: monaco.languages.CompletionItemKind.Function, insertText: 'NOT(${1:EventType})', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, documentation: 'Negation - event must not occur' },

        // Stream clauses
        { label: 'from', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'from(${1:Connector})', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, documentation: 'Bind to a connector source: EventType.from(Connector, key: value)' },
        { label: 'where', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'where ${1:condition}', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet },
        { label: 'select', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'select {\n\t${1:field}: ${2:value}$0\n}', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet },
        { label: 'emit', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'emit to "${1:sink}"', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet },
        { label: 'within', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'within ${1:5m}', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet },
        { label: 'window', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'window ${1:tumbling}(${2:5m})', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet },
        { label: 'aggregate', kind: monaco.languages.CompletionItemKind.Keyword, insertText: 'aggregate {\n\t${1:count}: count(),\n\t$0\n}', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet },

        // Types
        { label: 'int', kind: monaco.languages.CompletionItemKind.TypeParameter, insertText: 'int' },
        { label: 'float', kind: monaco.languages.CompletionItemKind.TypeParameter, insertText: 'float' },
        { label: 'bool', kind: monaco.languages.CompletionItemKind.TypeParameter, insertText: 'bool' },
        { label: 'str', kind: monaco.languages.CompletionItemKind.TypeParameter, insertText: 'str' },
        { label: 'timestamp', kind: monaco.languages.CompletionItemKind.TypeParameter, insertText: 'timestamp' },
        { label: 'duration', kind: monaco.languages.CompletionItemKind.TypeParameter, insertText: 'duration' },
      ]

      return { suggestions }
    }
  })

  // Define dark theme for VPL
  monaco.editor.defineTheme('vpl-dark', {
    base: 'vs-dark',
    inherit: true,
    rules: [
      { token: 'keyword', foreground: '7C4DFF', fontStyle: 'bold' },
      { token: 'type', foreground: '00BFA5' },
      { token: 'type.identifier', foreground: '00BFA5' },
      { token: 'number', foreground: 'F78C6C' },
      { token: 'number.float', foreground: 'F78C6C' },
      { token: 'number.duration', foreground: 'FFCB6B' },
      { token: 'string', foreground: 'C3E88D' },
      { token: 'string.escape', foreground: '89DDFF' },
      { token: 'comment', foreground: '546E7A', fontStyle: 'italic' },
      { token: 'operator', foreground: '89DDFF' },
      { token: 'identifier', foreground: 'EEFFFF' },
      { token: 'delimiter', foreground: '89DDFF' },
    ],
    colors: {
      'editor.background': '#1E1E1E',
      'editor.foreground': '#EEFFFF',
      'editorLineNumber.foreground': '#546E7A',
      'editorCursor.foreground': '#7C4DFF',
      'editor.selectionBackground': '#7C4DFF44',
      'editor.lineHighlightBackground': '#00000050',
    }
  })
}
