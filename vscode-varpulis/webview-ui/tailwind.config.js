/** @type {import('tailwindcss').Config} */
export default {
    content: [
        "./index.html",
        "./src/**/*.{js,ts,jsx,tsx}",
    ],
    theme: {
        extend: {
            colors: {
                'vscode-bg': 'var(--vscode-editor-background, #1e1e1e)',
                'vscode-fg': 'var(--vscode-editor-foreground, #d4d4d4)',
                'vscode-border': 'var(--vscode-panel-border, #444)',
                'vscode-accent': 'var(--vscode-focusBorder, #007acc)',
                'vscode-button': 'var(--vscode-button-background, #0e639c)',
                'vscode-input': 'var(--vscode-input-background, #3c3c3c)',
            }
        },
    },
    plugins: [],
}
