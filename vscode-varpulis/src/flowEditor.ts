import * as vscode from 'vscode';

export class FlowEditorProvider implements vscode.CustomTextEditorProvider {
    public static readonly viewType = 'varpulis.flowEditor';

    constructor(private readonly context: vscode.ExtensionContext) {}

    public static register(context: vscode.ExtensionContext): vscode.Disposable {
        const provider = new FlowEditorProvider(context);
        return vscode.window.registerCustomEditorProvider(
            FlowEditorProvider.viewType,
            provider,
            {
                webviewOptions: { retainContextWhenHidden: true },
                supportsMultipleEditorsPerDocument: false,
            }
        );
    }

    public async resolveCustomTextEditor(
        document: vscode.TextDocument,
        webviewPanel: vscode.WebviewPanel,
        _token: vscode.CancellationToken
    ): Promise<void> {
        webviewPanel.webview.options = {
            enableScripts: true,
            localResourceRoots: [
                vscode.Uri.joinPath(this.context.extensionUri, 'out', 'webview'),
                vscode.Uri.joinPath(this.context.extensionUri, 'webview-ui', 'dist'),
            ],
        };

        webviewPanel.webview.html = this.getHtmlForWebview(webviewPanel.webview);

        // Handle messages from the webview
        webviewPanel.webview.onDidReceiveMessage(async (message) => {
            switch (message.type) {
                case 'save':
                    this.saveDocument(document, message.data);
                    break;
                case 'export':
                    this.exportVPL(message.data);
                    break;
                case 'run':
                    this.runVPL(message.data);
                    break;
                case 'importVPL':
                    const vplUri = await vscode.window.showOpenDialog({
                        canSelectFiles: true,
                        canSelectFolders: false,
                        canSelectMany: false,
                        filters: { 'VPL': ['vpl'] },
                        openLabel: 'Import VPL',
                    });
                    if (vplUri && vplUri[0]) {
                        const vplContent = await vscode.workspace.fs.readFile(vplUri[0]);
                        webviewPanel.webview.postMessage({
                            type: 'loadVPL',
                            data: Buffer.from(vplContent).toString('utf-8'),
                        });
                        vscode.window.showInformationMessage(`Imported ${vplUri[0].fsPath}`);
                    }
                    break;
            }
        });

        // Update webview when document changes
        const changeDocumentSubscription = vscode.workspace.onDidChangeTextDocument((e) => {
            if (e.document.uri.toString() === document.uri.toString()) {
                webviewPanel.webview.postMessage({
                    type: 'update',
                    data: document.getText(),
                });
            }
        });

        webviewPanel.onDidDispose(() => {
            changeDocumentSubscription.dispose();
        });

        // Send initial document content
        if (document.getText()) {
            webviewPanel.webview.postMessage({
                type: 'load',
                data: document.getText(),
            });
        }
    }

    private getHtmlForWebview(webview: vscode.Webview): string {
        // Try to load React Flow webview from dist
        const distPath = vscode.Uri.joinPath(this.context.extensionUri, 'webview-ui', 'dist');

        try {
            const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(distPath, 'assets', 'index.js'));
            const styleUri = webview.asWebviewUri(vscode.Uri.joinPath(distPath, 'assets', 'index.css'));
            const nonce = getNonce();

            return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}'; img-src ${webview.cspSource} data:; font-src ${webview.cspSource};">
    <link rel="stylesheet" href="${styleUri}">
    <title>Varpulis Flow Editor</title>
</head>
<body>
    <div id="root"></div>
    <script nonce="${nonce}">
        window.vscode = acquireVsCodeApi();
    </script>
    <script nonce="${nonce}" type="module" src="${scriptUri}"></script>
</body>
</html>`;
        } catch {
            // Fallback to inline editor
            const nonce = getNonce();
            return getInlineEditorHtml(webview, nonce);
        }
    }

    private async saveDocument(document: vscode.TextDocument, content: string): Promise<void> {
        const edit = new vscode.WorkspaceEdit();
        edit.replace(
            document.uri,
            new vscode.Range(0, 0, document.lineCount, 0),
            content
        );
        await vscode.workspace.applyEdit(edit);
    }

    private async exportVPL(vplCode: string): Promise<void> {
        const uri = await vscode.window.showSaveDialog({
            filters: { 'VPL': ['vpl'] },
            saveLabel: 'Export VPL',
        });

        if (uri) {
            await vscode.workspace.fs.writeFile(uri, Buffer.from(vplCode, 'utf-8'));
            vscode.window.showInformationMessage(`Exported to ${uri.fsPath}`);
        }
    }

    private async runVPL(vplCode: string): Promise<void> {
        // Execute the run command with the generated VPL
        vscode.commands.executeCommand('varpulis.runCode', vplCode);
    }
}

export class FlowEditorPanel {
    public static currentPanel: FlowEditorPanel | undefined;
    private readonly _panel: vscode.WebviewPanel;
    private readonly _extensionUri: vscode.Uri;
    private _disposables: vscode.Disposable[] = [];

    public static createOrShow(extensionUri: vscode.Uri) {
        const column = vscode.window.activeTextEditor
            ? vscode.window.activeTextEditor.viewColumn
            : undefined;

        if (FlowEditorPanel.currentPanel) {
            FlowEditorPanel.currentPanel._panel.reveal(column);
            return;
        }

        const panel = vscode.window.createWebviewPanel(
            'varpulisFlowEditor',
            'Varpulis Flow Editor',
            column || vscode.ViewColumn.One,
            {
                enableScripts: true,
                retainContextWhenHidden: true,
                localResourceRoots: [
                    vscode.Uri.joinPath(extensionUri, 'out', 'webview'),
                    vscode.Uri.joinPath(extensionUri, 'webview-ui', 'dist'),
                ],
            }
        );

        FlowEditorPanel.currentPanel = new FlowEditorPanel(panel, extensionUri);
    }

    private constructor(panel: vscode.WebviewPanel, extensionUri: vscode.Uri) {
        this._panel = panel;
        this._extensionUri = extensionUri;

        this._update();

        this._panel.onDidDispose(() => this.dispose(), null, this._disposables);

        this._panel.webview.onDidReceiveMessage(
            async (message) => {
                switch (message.type) {
                    case 'export':
                        this._exportVPL(message.data);
                        break;
                    case 'run':
                        this._runVPL(message.data);
                        break;
                    case 'save':
                        this._saveFlow(message.data);
                        break;
                    case 'importVPL':
                        const vplUri = await vscode.window.showOpenDialog({
                            canSelectFiles: true,
                            canSelectFolders: false,
                            canSelectMany: false,
                            filters: { 'VPL': ['vpl'] },
                            openLabel: 'Import VPL',
                        });
                        if (vplUri && vplUri[0]) {
                            const vplContent = await vscode.workspace.fs.readFile(vplUri[0]);
                            this._panel.webview.postMessage({
                                type: 'loadVPL',
                                data: Buffer.from(vplContent).toString('utf-8'),
                            });
                            vscode.window.showInformationMessage(`Imported ${vplUri[0].fsPath}`);
                        }
                        break;
                }
            },
            null,
            this._disposables
        );
    }

    public dispose() {
        FlowEditorPanel.currentPanel = undefined;
        this._panel.dispose();
        while (this._disposables.length) {
            const x = this._disposables.pop();
            if (x) {
                x.dispose();
            }
        }
    }

    private _update() {
        this._panel.webview.html = this._getHtmlForWebview();
    }

    private _getHtmlForWebview(): string {
        const webview = this._panel.webview;

        // Try to load React Flow webview from dist
        const distPath = vscode.Uri.joinPath(this._extensionUri, 'webview-ui', 'dist');
        const indexHtmlUri = vscode.Uri.joinPath(distPath, 'index.html');

        try {
            // Check if React Flow build exists by constructing URIs
            const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(distPath, 'assets', 'index.js'));
            const styleUri = webview.asWebviewUri(vscode.Uri.joinPath(distPath, 'assets', 'index.css'));
            const nonce = getNonce();

            return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}'; img-src ${webview.cspSource} data:; font-src ${webview.cspSource};">
    <link rel="stylesheet" href="${styleUri}">
    <title>Varpulis Flow Editor</title>
</head>
<body>
    <div id="root"></div>
    <script nonce="${nonce}">
        window.vscode = acquireVsCodeApi();
    </script>
    <script nonce="${nonce}" type="module" src="${scriptUri}"></script>
</body>
</html>`;
        } catch {
            // Fallback to inline editor
            const nonce = getNonce();
            return getInlineEditorHtml(webview, nonce);
        }
    }

    private async _exportVPL(vplCode: string): Promise<void> {
        const uri = await vscode.window.showSaveDialog({
            filters: { 'VPL': ['vpl'] },
            saveLabel: 'Export VPL',
        });

        if (uri) {
            await vscode.workspace.fs.writeFile(uri, Buffer.from(vplCode, 'utf-8'));
            vscode.window.showInformationMessage(`Exported to ${uri.fsPath}`);
        }
    }

    private async _runVPL(vplCode: string): Promise<void> {
        vscode.commands.executeCommand('varpulis.runCode', vplCode);
    }

    private async _saveFlow(flowJson: string): Promise<void> {
        const uri = await vscode.window.showSaveDialog({
            filters: { 'Varpulis Flow': ['vflow', 'json'] },
            saveLabel: 'Save Flow',
        });

        if (uri) {
            await vscode.workspace.fs.writeFile(uri, Buffer.from(flowJson, 'utf-8'));
            vscode.window.showInformationMessage(`Saved to ${uri.fsPath}`);
        }
    }
}

function getNonce(): string {
    let text = '';
    const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    for (let i = 0; i < 32; i++) {
        text += possible.charAt(Math.floor(Math.random() * possible.length));
    }
    return text;
}

function getInlineEditorHtml(_webview: vscode.Webview, _nonce: string): string {
    return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Varpulis Flow Editor</title>
    <style>
        body {
            background: var(--vscode-editor-background, #1e1e1e);
            color: var(--vscode-editor-foreground, #d4d4d4);
            font-family: var(--vscode-font-family, sans-serif);
            display: flex;
            align-items: center;
            justify-content: center;
            height: 100vh;
            margin: 0;
        }
        .message {
            text-align: center;
            max-width: 480px;
            padding: 40px;
        }
        h2 { margin-bottom: 16px; }
        code {
            display: block;
            background: var(--vscode-textCodeBlock-background, #2d2d2d);
            padding: 12px 16px;
            border-radius: 4px;
            margin: 12px 0;
            font-size: 13px;
            text-align: left;
        }
        p { line-height: 1.6; color: var(--vscode-descriptionForeground, #888); }
    </style>
</head>
<body>
    <div class="message">
        <h2>Flow Editor Not Built</h2>
        <p>The graphical flow editor requires the webview UI to be built first:</p>
        <code>cd vscode-varpulis/webview-ui<br>npm install<br>npm run build</code>
        <p>Then reload the VS Code window.</p>
    </div>
</body>
</html>`;
}

