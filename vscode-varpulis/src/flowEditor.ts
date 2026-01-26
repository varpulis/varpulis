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
                        filters: { 'VarpulisQL': ['vpl'] },
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
        const nonce = getNonce();
        return getInlineEditorHtml(webview, nonce);
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
            filters: { 'VarpulisQL': ['vpl'] },
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
                            filters: { 'VarpulisQL': ['vpl'] },
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
        const nonce = getNonce();
        return getInlineEditorHtml(this._panel.webview, nonce);
    }

    private async _exportVPL(vplCode: string): Promise<void> {
        const uri = await vscode.window.showSaveDialog({
            filters: { 'VarpulisQL': ['vpl'] },
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

function getInlineEditorHtml(webview: vscode.Webview, nonce: string): string {
    return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}'; img-src ${webview.cspSource} data:;">
    <title>Varpulis Flow Editor</title>
    <style>
        :root {
            --bg: var(--vscode-editor-background, #1e1e1e);
            --fg: var(--vscode-editor-foreground, #d4d4d4);
            --border: var(--vscode-panel-border, #444);
            --accent: var(--vscode-focusBorder, #007acc);
            --btn: var(--vscode-button-background, #0e639c);
            --input: var(--vscode-input-background, #3c3c3c);
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { 
            background: var(--bg); 
            color: var(--fg); 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            height: 100vh;
            display: flex;
            flex-direction: column;
        }
        .toolbar {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 8px 16px;
            background: rgba(0,0,0,0.3);
            border-bottom: 1px solid var(--border);
        }
        .toolbar-group { display: flex; gap: 4px; align-items: center; }
        .toolbar-label { font-size: 12px; color: #888; margin-right: 8px; }
        .btn {
            display: flex; align-items: center; gap: 6px;
            padding: 6px 12px; font-size: 12px;
            background: transparent; color: var(--fg);
            border: none; border-radius: 4px; cursor: pointer;
        }
        .btn:hover { background: rgba(255,255,255,0.1); }
        .btn-primary { background: var(--btn); color: white; }
        .btn-primary:hover { opacity: 0.9; }
        .btn-danger { color: #f87171; }
        .btn-danger:hover { background: rgba(248,113,113,0.2); }
        .btn-source { color: #4ade80; }
        .btn-sink { color: #facc15; }
        .btn-event { color: #fbbf24; }
        .btn-stream { color: #818cf8; }
        .btn-pattern { color: #2dd4bf; }
        
        .main { display: flex; flex: 1; overflow: hidden; }
        
        .sidebar {
            width: 220px;
            background: rgba(0,0,0,0.2);
            border-right: 1px solid var(--border);
            overflow-y: auto;
        }
        .sidebar-header {
            padding: 12px;
            border-bottom: 1px solid var(--border);
        }
        .sidebar-header h2 { font-size: 13px; font-weight: 600; color: #ccc; }
        .sidebar-header p { font-size: 11px; color: #666; margin-top: 4px; }
        
        .category { border-bottom: 1px solid rgba(255,255,255,0.05); }
        .category-header {
            display: flex; align-items: center; gap: 8px;
            padding: 8px 12px; cursor: pointer;
            font-size: 13px;
        }
        .category-header:hover { background: rgba(255,255,255,0.05); }
        .category-items { padding-bottom: 8px; }
        .drag-item {
            display: flex; align-items: center; gap: 8px;
            padding: 6px 16px; margin: 2px 8px;
            font-size: 12px; color: #ccc;
            border-radius: 4px; cursor: grab;
        }
        .drag-item:hover { background: rgba(255,255,255,0.1); }
        .drag-item:active { cursor: grabbing; }
        
        .canvas-container { flex: 1; position: relative; overflow: hidden; }
        .canvas {
            width: 100%; height: 100%;
            background-image: radial-gradient(circle, #444 1px, transparent 1px);
            background-size: 20px 20px;
        }
        
        .node {
            position: absolute;
            min-width: 160px;
            padding: 12px 16px;
            border-radius: 8px;
            border: 2px solid;
            cursor: move;
            user-select: none;
        }
        .node.selected { box-shadow: 0 0 0 2px var(--accent); }
        .node-header { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }
        .node-icon { padding: 6px; background: rgba(0,0,0,0.3); border-radius: 4px; }
        .node-label { font-weight: 600; font-size: 13px; }
        .node-type { font-size: 10px; text-transform: uppercase; color: #888; letter-spacing: 0.5px; }
        .node-config { font-size: 11px; color: #888; margin-top: 4px; }
        
        .node-source { border-color: #22c55e; background: rgba(34,197,94,0.15); }
        .node-sink { border-color: #eab308; background: rgba(234,179,8,0.15); }
        .node-event { border-color: #f59e0b; background: rgba(245,158,11,0.15); }
        .node-stream { border-color: #6366f1; background: rgba(99,102,241,0.15); }
        .node-pattern { border-color: #f472b6; background: rgba(244,114,182,0.15); }
        .node-function { border-color: #34d399; background: rgba(52,211,153,0.15); }
        
        .handle {
            position: absolute;
            width: 12px; height: 12px;
            border-radius: 50%;
            border: 2px solid var(--bg);
        }
        .handle-source { right: -6px; top: 50%; transform: translateY(-50%); background: #22c55e; cursor: crosshair; }
        .handle-target { left: -6px; top: 50%; transform: translateY(-50%); background: #ef4444; cursor: crosshair; }
        .handle:hover { transform: translateY(-50%) scale(1.3); }
        
        .node-delete {
            position: absolute; top: -8px; right: -8px;
            width: 20px; height: 20px;
            border-radius: 50%; border: none;
            background: #ef4444; color: white;
            font-size: 14px; font-weight: bold;
            cursor: pointer; display: none;
            align-items: center; justify-content: center;
            line-height: 1;
        }
        .node:hover .node-delete { display: flex; }
        .node-delete:hover { background: #dc2626; transform: scale(1.1); }
        
        .connection {
            position: absolute;
            pointer-events: none;
        }
        .connection line {
            stroke: #666;
            stroke-width: 2;
            stroke-dasharray: 5,5;
        }
        
        .panel {
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0,0,0,0.5);
            display: flex; align-items: center; justify-content: center;
            z-index: 100;
        }
        .panel-content {
            background: var(--bg);
            border: 1px solid var(--border);
            border-radius: 8px;
            width: 480px;
            max-height: 80vh;
            overflow: hidden;
        }
        .panel-header {
            display: flex; align-items: center; justify-content: space-between;
            padding: 12px 16px;
            border-bottom: 1px solid var(--border);
            font-weight: 600;
        }
        .panel-body { padding: 16px; overflow-y: auto; max-height: 60vh; }
        .panel-footer {
            display: flex; justify-content: flex-end; gap: 8px;
            padding: 12px 16px;
            border-top: 1px solid var(--border);
        }
        
        .form-group { margin-bottom: 16px; }
        .form-label { display: block; font-size: 12px; color: #888; margin-bottom: 4px; }
        .form-input {
            width: 100%;
            padding: 8px 12px;
            background: var(--input);
            border: 1px solid var(--border);
            border-radius: 4px;
            color: var(--fg);
            font-size: 13px;
        }
        .form-input:focus { border-color: var(--accent); outline: none; }
        .form-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
        
        .connector-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin-bottom: 16px; }
        .connector-btn {
            display: flex; flex-direction: column; align-items: center; gap: 4px;
            padding: 12px; border-radius: 4px;
            border: 1px solid var(--border);
            background: transparent; color: var(--fg);
            cursor: pointer; font-size: 11px;
        }
        .connector-btn:hover { border-color: #666; }
        .connector-btn.active { border-color: var(--accent); background: rgba(0,122,204,0.2); }
        
        .svg-icon { width: 16px; height: 16px; }
        .hidden { display: none !important; }
    </style>
</head>
<body>
    <div class="toolbar">
        <div class="toolbar-group">
            <span class="toolbar-label">Add:</span>
            <button class="btn btn-source" data-action="addNode" data-type="source">
                <svg class="svg-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 8v8M8 12h8"/></svg>
                Source
            </button>
            <button class="btn btn-sink" data-action="addNode" data-type="sink">
                <svg class="svg-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="4" y="4" width="16" height="16" rx="2"/><path d="M8 12h8"/></svg>
                Sink
            </button>
            <button class="btn btn-event" data-action="addNode" data-type="event">
                <svg class="svg-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
                Event
            </button>
            <button class="btn btn-stream" data-action="addNode" data-type="stream">
                <svg class="svg-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></svg>
                Stream
            </button>
        </div>
        <div class="toolbar-group">
            <button class="btn btn-primary" data-action="runFlow">
                <svg class="svg-icon" viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg>
                Run
            </button>
            <button class="btn" data-action="exportVPL">
                <svg class="svg-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
                Export
            </button>
            <button class="btn" data-action="saveFlow">
                <svg class="svg-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>
                Save
            </button>
            <button class="btn" data-action="importVPL">
                <svg class="svg-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
                Import VPL
            </button>
            <button class="btn btn-danger" data-action="clearAll">
                <svg class="svg-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
            </button>
        </div>
    </div>
    
    <div class="main">
        <div class="sidebar">
            <div class="sidebar-header">
                <h2>Components</h2>
                <p>Drag to canvas</p>
            </div>
            <div class="category">
                <div class="category-header">
                    <svg class="svg-icon" style="color:#4ade80" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 8v8M8 12h8"/></svg>
                    Sources
                </div>
                <div class="category-items">
                    <div class="drag-item" draggable="true" data-type="source" data-connector="mqtt">
                        <svg class="svg-icon" style="color:#4ade80" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4.9 19.1C1 15.2 1 8.8 4.9 4.9"/><path d="M7.8 16.2c-2.3-2.3-2.3-6.1 0-8.5"/><circle cx="12" cy="12" r="2"/><path d="M16.2 7.8c2.3 2.3 2.3 6.1 0 8.5"/><path d="M19.1 4.9C23 8.8 23 15.1 19.1 19"/></svg>
                        MQTT Source
                    </div>
                    <div class="drag-item" draggable="true" data-type="source" data-connector="kafka">
                        <svg class="svg-icon" style="color:#fb923c" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>
                        Kafka Source
                    </div>
                    <div class="drag-item" draggable="true" data-type="source" data-connector="file">
                        <svg class="svg-icon" style="color:#60a5fa" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>
                        File Source
                    </div>
                </div>
            </div>
            <div class="category">
                <div class="category-header">
                    <svg class="svg-icon" style="color:#facc15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg>
                    Sinks
                </div>
                <div class="category-items">
                    <div class="drag-item" draggable="true" data-type="sink" data-connector="console">
                        <svg class="svg-icon" style="color:#facc15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg>
                        Console
                    </div>
                    <div class="drag-item" draggable="true" data-type="sink" data-connector="mqtt">
                        <svg class="svg-icon" style="color:#4ade80" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4.9 19.1C1 15.2 1 8.8 4.9 4.9"/><path d="M7.8 16.2c-2.3-2.3-2.3-6.1 0-8.5"/><circle cx="12" cy="12" r="2"/><path d="M16.2 7.8c2.3 2.3 2.3 6.1 0 8.5"/><path d="M19.1 4.9C23 8.8 23 15.1 19.1 19"/></svg>
                        MQTT Sink
                    </div>
                    <div class="drag-item" draggable="true" data-type="sink" data-connector="file">
                        <svg class="svg-icon" style="color:#60a5fa" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>
                        File Sink
                    </div>
                </div>
            </div>
            <div class="category">
                <div class="category-header">
                    <svg class="svg-icon" style="color:#fbbf24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
                    Events
                </div>
                <div class="category-items">
                    <div class="drag-item" draggable="true" data-type="event">
                        <svg class="svg-icon" style="color:#fbbf24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
                        Event Type
                    </div>
                </div>
            </div>
            <div class="category">
                <div class="category-header">
                    <svg class="svg-icon" style="color:#818cf8" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></svg>
                    Streams
                </div>
                <div class="category-items">
                    <div class="drag-item" draggable="true" data-type="stream">
                        <svg class="svg-icon" style="color:#818cf8" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></svg>
                        Stream
                    </div>
                </div>
            </div>
            <div class="category">
                <div class="category-header">
                    <svg class="svg-icon" style="color:#f472b6" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></svg>
                    Patterns
                </div>
                <div class="category-items">
                    <div class="drag-item" draggable="true" data-type="pattern">
                        <svg class="svg-icon" style="color:#f472b6" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="6" cy="12" r="3"/><circle cx="18" cy="12" r="3"/><path d="M9 12h6"/><path d="M15 9l3 3-3 3"/></svg>
                        Sequence (A -> B)
                    </div>
                    <div class="drag-item" draggable="true" data-type="pattern" data-pattern="attention">
                        <svg class="svg-icon" style="color:#a78bfa" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>
                        Attention Window
                    </div>
                </div>
            </div>
            <div class="category">
                <div class="category-header">
                    <svg class="svg-icon" style="color:#34d399" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/></svg>
                    Functions
                </div>
                <div class="category-items">
                    <div class="drag-item" draggable="true" data-type="function">
                        <svg class="svg-icon" style="color:#34d399" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
                        Function
                    </div>
                </div>
            </div>
            <div class="category">
                <div class="category-header">
                    <svg class="svg-icon" style="color:#888" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/></svg>
                    Aide
                </div>
                <div class="category-items">
                    <div class="help-text" style="padding:8px 16px;font-size:11px;color:#888;line-height:1.5">
                        <p><b>Patterns VPL :</b></p>
                        <p style="margin-top:4px">• A -> B (séquence)</p>
                        <p>• A and B (conjonction)</p>
                        <p>• A or B (disjonction)</p>
                        <p>• not A (négation)</p>
                        <p>• A* (Kleene star)</p>
                        <p style="margin-top:8px"><b>Raccourcis :</b></p>
                        <p>• Double-clic : configurer</p>
                        <p>• Delete : supprimer</p>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="canvas-container">
            <div class="canvas" id="canvas"></div>
            <svg class="connection" id="connections" style="position:absolute;top:0;left:0;width:100%;height:100%;pointer-events:none;"></svg>
        </div>
    </div>
    
    <div class="panel hidden" id="configPanel">
        <div class="panel-content">
            <div class="panel-header">
                <span id="panelTitle">Configure Node</span>
                <button class="btn" id="closePanelBtn">&times;</button>
            </div>
            <div class="panel-body" id="panelBody"></div>
            <div class="panel-footer">
                <button type="button" class="btn" id="cancelBtn">Cancel</button>
                <button type="button" class="btn btn-primary" id="saveBtn">Save</button>
            </div>
        </div>
    </div>

    <script nonce="${nonce}">
        const vscode = acquireVsCodeApi();
        let nodes = [];
        let connections = [];
        let selectedNode = null;
        let nodeId = 0;
        let draggingNode = null;
        let dragOffset = { x: 0, y: 0 };
        let connectingFrom = null;
        
        const canvas = document.getElementById('canvas');
        const connSvg = document.getElementById('connections');
        
        // Panel buttons
        document.getElementById('cancelBtn').addEventListener('click', closePanel);
        document.getElementById('saveBtn').addEventListener('click', saveConfig);
        document.getElementById('closePanelBtn').addEventListener('click', closePanel);
        
        // Toolbar buttons with data-action
        document.querySelectorAll('[data-action]').forEach(btn => {
            btn.addEventListener('click', function() {
                const action = this.dataset.action;
                const type = this.dataset.type;
                switch(action) {
                    case 'addNode': addNode(type); break;
                    case 'runFlow': runFlow(); break;
                    case 'exportVPL': exportVPL(); break;
                    case 'saveFlow': saveFlow(); break;
                    case 'importVPL': importVPL(); break;
                    case 'clearAll': clearAll(); break;
                }
            });
        });
        
        // Drag from sidebar
        document.querySelectorAll('.drag-item').forEach(item => {
            item.addEventListener('dragstart', e => {
                e.dataTransfer.setData('type', item.dataset.type);
                e.dataTransfer.setData('connector', item.dataset.connector || '');
                e.dataTransfer.setData('pattern', item.dataset.pattern || '');
            });
        });
        
        canvas.addEventListener('dragover', e => e.preventDefault());
        canvas.addEventListener('drop', e => {
            e.preventDefault();
            const type = e.dataTransfer.getData('type');
            const connector = e.dataTransfer.getData('connector');
            const pattern = e.dataTransfer.getData('pattern');
            const rect = canvas.getBoundingClientRect();
            const x = e.clientX - rect.left - 80;
            const y = e.clientY - rect.top - 40;
            createNode(type, x, y, { connector, pattern });
        });
        
        function createNode(type, x, y, extra = {}) {
            const id = 'node_' + (nodeId++);
            const node = {
                id,
                type,
                x,
                y,
                label: type.charAt(0).toUpperCase() + type.slice(1) + ' ' + nodeId,
                connector: extra.connector || null,
                pattern: extra.pattern || null,
                config: {}
            };
            nodes.push(node);
            renderNode(node);
            return node;
        }
        
        function renderNode(node) {
            const el = document.createElement('div');
            el.className = 'node node-' + node.type;
            el.id = node.id;
            el.style.left = node.x + 'px';
            el.style.top = node.y + 'px';
            
            let configHtml = '';
            if (node.connector) {
                configHtml = '<div class="node-config">' + node.connector.toUpperCase() + '</div>';
            }
            if (node.config && node.config.pattern) {
                configHtml = '<div class="node-config">' + node.config.pattern + '</div>';
            }
            
            el.innerHTML = \`
                <button class="node-delete" data-delete="\${node.id}" title="Supprimer">&times;</button>
                <div class="node-header">
                    <div class="node-label">\${node.label}</div>
                </div>
                <div class="node-type">\${node.type}</div>
                \${configHtml}
                \${node.type !== 'source' ? '<div class="handle handle-target" data-handle="target"></div>' : ''}
                \${node.type !== 'sink' ? '<div class="handle handle-source" data-handle="source"></div>' : ''}
            \`;
            
            // Delete button
            el.querySelector('.node-delete').addEventListener('click', e => {
                e.stopPropagation();
                deleteNode(node.id);
            });
            
            // Drag node
            el.addEventListener('mousedown', e => {
                if (e.target.classList.contains('handle') || e.target.classList.contains('node-delete')) return;
                draggingNode = node;
                dragOffset = { x: e.offsetX, y: e.offsetY };
                document.querySelectorAll('.node').forEach(n => n.classList.remove('selected'));
                el.classList.add('selected');
                selectedNode = node;
            });
            
            // Handle connections
            el.querySelectorAll('.handle').forEach(h => {
                h.addEventListener('mousedown', e => {
                    e.stopPropagation();
                    if (h.dataset.handle === 'source') {
                        connectingFrom = { node, el };
                    }
                });
                h.addEventListener('mouseup', e => {
                    e.stopPropagation();
                    if (connectingFrom && h.dataset.handle === 'target' && connectingFrom.node.id !== node.id) {
                        // Avoid duplicate connections
                        if (!connections.find(c => c.from === connectingFrom.node.id && c.to === node.id)) {
                            connections.push({ from: connectingFrom.node.id, to: node.id });
                            renderConnections();
                        }
                    }
                    connectingFrom = null;
                });
            });
            
            // Double-click to configure
            el.addEventListener('dblclick', () => openConfig(node));
            
            canvas.appendChild(el);
        }
        
        function deleteNode(id) {
            nodes = nodes.filter(n => n.id !== id);
            connections = connections.filter(c => c.from !== id && c.to !== id);
            const el = document.getElementById(id);
            if (el) el.remove();
            renderConnections();
            if (selectedNode && selectedNode.id === id) selectedNode = null;
        }
        
        document.addEventListener('mousemove', e => {
            if (draggingNode) {
                const rect = canvas.getBoundingClientRect();
                draggingNode.x = e.clientX - rect.left - dragOffset.x;
                draggingNode.y = e.clientY - rect.top - dragOffset.y;
                const el = document.getElementById(draggingNode.id);
                el.style.left = draggingNode.x + 'px';
                el.style.top = draggingNode.y + 'px';
                renderConnections();
            }
        });
        
        document.addEventListener('mouseup', () => {
            draggingNode = null;
            connectingFrom = null;
        });
        
        // Keyboard delete
        document.addEventListener('keydown', e => {
            if (e.key === 'Delete' || e.key === 'Backspace') {
                if (selectedNode && document.activeElement.tagName !== 'INPUT') {
                    deleteNode(selectedNode.id);
                }
            }
        });
        
        function renderConnections() {
            connSvg.innerHTML = connections.map(c => {
                const fromEl = document.getElementById(c.from);
                const toEl = document.getElementById(c.to);
                if (!fromEl || !toEl) return '';
                const fromRect = fromEl.getBoundingClientRect();
                const toRect = toEl.getBoundingClientRect();
                const canvasRect = canvas.getBoundingClientRect();
                const x1 = fromRect.right - canvasRect.left;
                const y1 = fromRect.top + fromRect.height/2 - canvasRect.top;
                const x2 = toRect.left - canvasRect.left;
                const y2 = toRect.top + toRect.height/2 - canvasRect.top;
                const dx = Math.abs(x2 - x1) * 0.5;
                const path = 'M '+x1+' '+y1+' C '+(x1+dx)+' '+y1+', '+(x2-dx)+' '+y2+', '+x2+' '+y2;
                return '<path d="'+path+'" fill="none" stroke="#22c55e" stroke-width="2" marker-end="url(#arrow)"/>';
            }).join('') + '<defs><marker id="arrow" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto"><path d="M0,0 L0,6 L9,3 z" fill="#22c55e"/></marker></defs>';
        }
        
        function addNode(type) {
            createNode(type, 250 + Math.random()*100, 100 + Math.random()*100);
        }
        
        function openConfig(node) {
            selectedNode = node;
            document.getElementById('panelTitle').textContent = 'Configure ' + node.label;
            let html = '<div class="form-group"><label class="form-label">Name</label><input class="form-input" id="cfg-name" value="'+node.label+'"></div>';
            
            if (node.type === 'source' || node.type === 'sink') {
                html += '<div class="form-group"><label class="form-label">Connector</label><div class="connector-grid">';
                ['mqtt', 'kafka', 'amqp', 'file', 'http'].forEach(c => {
                    if (node.type === 'sink' || c !== 'console') {
                        html += '<button type="button" class="connector-btn '+(node.connector===c?'active':'')+' " data-connector="'+c+'">'+c.toUpperCase()+'</button>';
                    }
                });
                if (node.type === 'sink') {
                    html += '<button type="button" class="connector-btn '+(node.connector==='console'?'active':'')+' " data-connector="console">CONSOLE</button>';
                }
                html += '</div></div>';
                
                html += '<div id="connector-fields">' + getConnectorFields(node.connector, node.config) + '</div>';
            }
            
            if (node.type === 'event') {
                html += '<div class="form-group"><label class="form-label">Fields (one per line: name: type)</label><textarea class="form-input" id="cfg-fields" rows="4" style="resize:vertical" placeholder="value: float\\ntimestamp: datetime">'+(node.config.fields||'')+'</textarea></div>';
            }
            
            if (node.type === 'stream') {
                html += '<div class="form-group"><label class="form-label">Source Event/Stream</label><input class="form-input" id="cfg-source" value="'+(node.config.source||'')+'"></div>';
                html += '<div class="form-group"><input type="checkbox" id="cfg-isBase" '+(node.config.isBase?'checked':'')+' style="margin-right:8px"><label for="cfg-isBase">Base stream (just "from" declaration)</label></div>';
                html += '<div class="form-group"><label class="form-label">Alias (ex: "as e" for referencing)</label><input class="form-input" id="cfg-alias" value="'+(node.config.alias||'')+'"></div>';
                html += '<div class="form-group"><label class="form-label">Partition By (field name)</label><input class="form-input" id="cfg-partitionBy" value="'+(node.config.partitionBy||'')+'"></div>';
                html += '<div class="form-group"><label class="form-label">Window (ex: 5m, 1h, 10s)</label><input class="form-input" id="cfg-window" value="'+(node.config.window||'')+'"></div>';
                html += '<div class="form-group"><label class="form-label">Where (filter condition)</label><input class="form-input" id="cfg-where" value="'+(node.config.where||'')+'"></div>';
                html += '<div class="form-group"><label class="form-label">Sequence Pattern (ex: A -> B where b.id == a.id)</label><input class="form-input" id="cfg-sequence" value="'+(node.config.sequence||'')+'"></div>';
                html += '<div class="form-group"><label class="form-label">Aggregate (JSON: {"avg_val": "avg(value)"})</label><textarea class="form-input" id="cfg-aggregate" rows="3">'+(node.config.aggregate?JSON.stringify(node.config.aggregate,null,2):'')+'</textarea></div>';
                html += '<div class="form-group"><label class="form-label">Emit (JSON: {"alert": "value"})</label><textarea class="form-input" id="cfg-emit" rows="3">'+(node.config.emit?JSON.stringify(node.config.emit,null,2):'')+'</textarea></div>';
                html += '<hr style="border-color:#333;margin:16px 0">';
                html += '<div class="form-group"><label class="form-label" style="color:#a78bfa">Attention Window (optional)</label></div>';
                html += '<div class="form-group"><label class="form-label">Attention Size</label><input class="form-input" id="cfg-attentionSize" value="'+(node.config.attentionSize||'')+'"></div>';
                html += '<div class="form-group"><label class="form-label">Attention Pattern (ex: degradation)</label><input class="form-input" id="cfg-attentionPattern" value="'+(node.config.attentionPattern||'')+'"></div>';
                html += '<div class="form-group"><label class="form-label">Attention Threshold</label><input class="form-input" id="cfg-attentionThreshold" value="'+(node.config.attentionThreshold||'')+'"></div>';
            }
            
            if (node.type === 'pattern') {
                html += '<div class="form-group"><label class="form-label">Pattern Type</label><select class="form-input" id="cfg-patternType"><option value="sequence" '+(node.config.patternType==='sequence'?'selected':'')+'>Sequence (->)</option><option value="attention" '+(node.config.patternType==='attention'?'selected':'')+'>Attention Window</option></select></div>';
                html += '<div class="form-group"><label class="form-label">Source Stream</label><input class="form-input" id="cfg-source" value="'+(node.config.source||'')+'"></div>';
                html += '<div class="form-group"><label class="form-label">Sequence (ex: Login as l -> AccessDenied where user == l.user -> AccessDenied)</label><textarea class="form-input" id="cfg-sequence" rows="3">'+(node.config.sequence||'')+'</textarea></div>';
                html += '<div class="form-group"><label class="form-label">Within (time constraint, ex: 5m)</label><input class="form-input" id="cfg-within" value="'+(node.config.within||'')+'"></div>';
                html += '<hr style="border-color:#333;margin:16px 0">';
                html += '<div class="form-group"><label class="form-label" style="color:#a78bfa">Attention Window Config</label></div>';
                html += '<div class="form-group"><label class="form-label">Size</label><input class="form-input" id="cfg-attentionSize" value="'+(node.config.attentionSize||'')+'"></div>';
                html += '<div class="form-group"><label class="form-label">Pattern (ex: degradation, anomaly)</label><input class="form-input" id="cfg-attentionPattern" value="'+(node.config.attentionPattern||'')+'"></div>';
                html += '<div class="form-group"><label class="form-label">Threshold (0.0 - 1.0)</label><input class="form-input" id="cfg-attentionThreshold" value="'+(node.config.attentionThreshold||'')+'"></div>';
                html += '<div class="form-group"><label class="form-label">Emit (JSON)</label><textarea class="form-input" id="cfg-emit" rows="3">'+(node.config.emit?JSON.stringify(node.config.emit,null,2):'')+'</textarea></div>';
            }
            
            if (node.type === 'function') {
                html += '<div class="form-group"><label class="form-label">Parameters (ex: a: int, b: float)</label><input class="form-input" id="cfg-params" value="'+(node.config.params||'')+'"></div>';
                html += '<div class="form-group"><label class="form-label">Return Type</label><input class="form-input" id="cfg-returnType" value="'+(node.config.returnType||'')+'"></div>';
                html += '<div class="form-group"><label class="form-label">Body (expression)</label><textarea class="form-input" id="cfg-body" rows="4">'+(node.config.body||'')+'</textarea></div>';
            }
            
            document.getElementById('panelBody').innerHTML = html;
            document.getElementById('configPanel').classList.remove('hidden');
            
            // Bind connector buttons
            document.querySelectorAll('.connector-btn').forEach(btn => {
                btn.addEventListener('click', function(e) {
                    e.preventDefault();
                    e.stopPropagation();
                    selectConnector(this.dataset.connector);
                });
            });
        }
        
        function getConnectorFields(connector, config) {
            config = config || {};
            if (connector === 'mqtt') {
                return '<div class="form-row"><div class="form-group"><label class="form-label">Host</label><input class="form-input" id="cfg-host" value="'+(config.host||'localhost')+'"></div><div class="form-group"><label class="form-label">Port</label><input class="form-input" id="cfg-port" value="'+(config.port||'1883')+'"></div></div><div class="form-group"><label class="form-label">Topic</label><input class="form-input" id="cfg-topic" value="'+(config.topic||'events')+'"></div>';
            }
            if (connector === 'kafka') {
                return '<div class="form-group"><label class="form-label">Brokers</label><input class="form-input" id="cfg-brokers" value="'+(config.brokers||'localhost:9092')+'"></div><div class="form-group"><label class="form-label">Topic</label><input class="form-input" id="cfg-topic" value="'+(config.topic||'events')+'"></div>';
            }
            if (connector === 'file') {
                return '<div class="form-group"><label class="form-label">Path</label><input class="form-input" id="cfg-path" value="'+(config.path||'./events.evt')+'"></div>';
            }
            return '';
        }
        
        function selectConnector(c) {
            selectedNode.connector = c;
            document.querySelectorAll('.connector-btn').forEach(b => {
                b.classList.remove('active');
                if (b.dataset.connector === c) b.classList.add('active');
            });
            document.getElementById('connector-fields').innerHTML = getConnectorFields(c, selectedNode.config);
        }
        
        function closePanel() {
            document.getElementById('configPanel').classList.add('hidden');
        }
        
        function saveConfig() {
            if (!selectedNode) return;
            selectedNode.label = document.getElementById('cfg-name')?.value || selectedNode.label;
            
            if (selectedNode.type === 'source' || selectedNode.type === 'sink') {
                selectedNode.config.host = document.getElementById('cfg-host')?.value;
                selectedNode.config.port = document.getElementById('cfg-port')?.value;
                selectedNode.config.topic = document.getElementById('cfg-topic')?.value;
                selectedNode.config.brokers = document.getElementById('cfg-brokers')?.value;
                selectedNode.config.path = document.getElementById('cfg-path')?.value;
            }
            if (selectedNode.type === 'stream') {
                selectedNode.config.source = document.getElementById('cfg-source')?.value;
                selectedNode.config.isBase = document.getElementById('cfg-isBase')?.checked;
                selectedNode.config.alias = document.getElementById('cfg-alias')?.value;
                selectedNode.config.partitionBy = document.getElementById('cfg-partitionBy')?.value;
                selectedNode.config.window = document.getElementById('cfg-window')?.value;
                selectedNode.config.where = document.getElementById('cfg-where')?.value;
                selectedNode.config.sequence = document.getElementById('cfg-sequence')?.value;
                selectedNode.config.attentionSize = document.getElementById('cfg-attentionSize')?.value;
                selectedNode.config.attentionPattern = document.getElementById('cfg-attentionPattern')?.value;
                selectedNode.config.attentionThreshold = document.getElementById('cfg-attentionThreshold')?.value;
                try {
                    const aggVal = document.getElementById('cfg-aggregate')?.value;
                    selectedNode.config.aggregate = aggVal ? JSON.parse(aggVal) : null;
                } catch(e) {}
                try {
                    const emitVal = document.getElementById('cfg-emit')?.value;
                    selectedNode.config.emit = emitVal ? JSON.parse(emitVal) : null;
                } catch(e) {}
            }
            if (selectedNode.type === 'event') {
                selectedNode.config.fields = document.getElementById('cfg-fields')?.value;
            }
            if (selectedNode.type === 'pattern') {
                selectedNode.config.patternType = document.getElementById('cfg-patternType')?.value;
                selectedNode.config.source = document.getElementById('cfg-source')?.value;
                selectedNode.config.sequence = document.getElementById('cfg-sequence')?.value;
                selectedNode.config.within = document.getElementById('cfg-within')?.value;
                selectedNode.config.attentionSize = document.getElementById('cfg-attentionSize')?.value;
                selectedNode.config.attentionPattern = document.getElementById('cfg-attentionPattern')?.value;
                selectedNode.config.attentionThreshold = document.getElementById('cfg-attentionThreshold')?.value;
                try {
                    const emitVal = document.getElementById('cfg-emit')?.value;
                    selectedNode.config.emit = emitVal ? JSON.parse(emitVal) : null;
                } catch(e) {}
            }
            if (selectedNode.type === 'function') {
                selectedNode.config.params = document.getElementById('cfg-params')?.value;
                selectedNode.config.returnType = document.getElementById('cfg-returnType')?.value;
                selectedNode.config.body = document.getElementById('cfg-body')?.value;
            }
            
            // Re-render node fully
            const el = document.getElementById(selectedNode.id);
            el.querySelector('.node-label').textContent = selectedNode.label;
            
            // Update config display
            let configEl = el.querySelector('.node-config');
            let configText = '';
            if (selectedNode.connector) configText = selectedNode.connector.toUpperCase();
            if (selectedNode.config.pattern) configText = selectedNode.config.pattern;
            
            if (configText) {
                if (!configEl) {
                    configEl = document.createElement('div');
                    configEl.className = 'node-config';
                    el.appendChild(configEl);
                }
                configEl.textContent = configText;
            }
            
            closePanel();
        }
        
        function generateVPL() {
            let vpl = '# Generated by Varpulis Flow Editor\\n# ' + new Date().toISOString() + '\\n\\n';
            
            const events = nodes.filter(n => n.type === 'event');
            const streams = nodes.filter(n => n.type === 'stream');
            
            // Generate event definitions
            if (events.length) {
                vpl += '# =============================================================================\\n';
                vpl += '# Event Definitions\\n';
                vpl += '# =============================================================================\\n\\n';
                events.forEach(e => {
                    vpl += 'event ' + e.label + ':\\n';
                    const fields = e.config.fields ? e.config.fields.trim() : '';
                    if (fields) {
                        fields.split('\\n').forEach(f => {
                            if (f.trim()) vpl += '    ' + f.trim() + '\\n';
                        });
                    } else {
                        vpl += '    id: str\\n';
                        vpl += '    ts: int\\n';
                    }
                    vpl += '\\n';
                });
            }
            
            // Generate streams
            if (streams.length) {
                vpl += '# =============================================================================\\n';
                vpl += '# Streams\\n';
                vpl += '# =============================================================================\\n\\n';
                streams.forEach(s => {
                    // Find source event/stream
                    const sourceConn = connections.find(c => c.to === s.id);
                    const sourceNode = sourceConn ? nodes.find(n => n.id === sourceConn.from) : null;
                    const sourceName = sourceNode ? sourceNode.label : (s.config.source || 'Event');
                    
                    // Check if this is a base stream (just "from" declaration)
                    if (s.config.isBase) {
                        vpl += 'stream ' + s.label + ' from ' + sourceName + '\\n\\n';
                        return;
                    }
                    
                    // Stream with operators
                    vpl += 'stream ' + s.label + ' = ' + sourceName;
                    if (s.config.alias) {
                        vpl += ' as ' + s.config.alias;
                    }
                    vpl += '\\n';
                    
                    // Operators
                    if (s.config.partitionBy) {
                        vpl += '    .partition_by(' + s.config.partitionBy + ')\\n';
                    }
                    if (s.config.window) {
                        vpl += '    .window(' + s.config.window + ')\\n';
                    }
                    if (s.config.where) {
                        vpl += '    .where(' + s.config.where + ')\\n';
                    }
                    if (s.config.sequence) {
                        vpl += '    ' + s.config.sequence + '\\n';
                    }
                    if (s.config.aggregate) {
                        vpl += '    .aggregate(\\n';
                        const entries = Object.entries(s.config.aggregate);
                        entries.forEach(([k, v], i) => {
                            vpl += '        ' + k + ': ' + v + (i < entries.length - 1 ? ',' : '') + '\\n';
                        });
                        vpl += '    )\\n';
                    }
                    if (s.config.attentionSize || s.config.attentionPattern) {
                        vpl += '    .attention_window(';
                        const attParts = [];
                        if (s.config.attentionSize) attParts.push('size: ' + s.config.attentionSize);
                        if (s.config.attentionPattern) attParts.push('pattern: ' + s.config.attentionPattern);
                        if (s.config.attentionThreshold) attParts.push('threshold: ' + s.config.attentionThreshold);
                        vpl += attParts.join(', ') + ')\\n';
                    }
                    if (s.config.print) {
                        vpl += '    .print(' + s.config.print + ')\\n';
                    }
                    if (s.config.emit) {
                        vpl += '    .emit(\\n';
                        const entries = Object.entries(s.config.emit);
                        entries.forEach(([k, v], i) => {
                            const isFieldRef = typeof v === 'string' && (v.match(/^[a-z_][a-z0-9_.]*$/i) || v.includes('('));
                            const val = isFieldRef ? v : '\"' + v + '\"';
                            vpl += '        ' + k + ': ' + val + (i < entries.length - 1 ? ',' : '') + '\\n';
                        });
                        vpl += '    )\\n';
                    }
                    vpl += '\\n';
                });
            }
            
            // Generate functions
            const functions = nodes.filter(n => n.type === 'function');
            if (functions.length) {
                vpl += '# =============================================================================\\n';
                vpl += '# Functions\\n';
                vpl += '# =============================================================================\\n\\n';
                functions.forEach(f => {
                    const params = f.config.params || '';
                    const returnType = f.config.returnType || 'int';
                    const body = f.config.body || '0';
                    vpl += 'fn ' + f.label + '(' + params + ') -> ' + returnType + ':\\n';
                    body.split('\\n').forEach(line => {
                        vpl += '    ' + line + '\\n';
                    });
                    vpl += '\\n';
                });
            }
            
            // Generate patterns
            const patterns = nodes.filter(n => n.type === 'pattern');
            if (patterns.length) {
                vpl += '# =============================================================================\\n';
                vpl += '# Patterns\\n';
                vpl += '# =============================================================================\\n\\n';
                patterns.forEach(p => {
                    const source = p.config.source || 'Event';
                    vpl += 'stream ' + p.label + ' = ' + source + '\\n';
                    if (p.config.sequence) {
                        vpl += '    ' + p.config.sequence + '\\n';
                    }
                    if (p.config.within) {
                        vpl += '    .within(' + p.config.within + ')\\n';
                    }
                    if (p.config.attentionSize || p.config.attentionPattern) {
                        vpl += '    .attention_window(';
                        const attParts = [];
                        if (p.config.attentionSize) attParts.push('size: ' + p.config.attentionSize);
                        if (p.config.attentionPattern) attParts.push('pattern: ' + p.config.attentionPattern);
                        if (p.config.attentionThreshold) attParts.push('threshold: ' + p.config.attentionThreshold);
                        vpl += attParts.join(', ') + ')\\n';
                    }
                    if (p.config.emit) {
                        vpl += '    .emit(\\n';
                        const entries = Object.entries(p.config.emit);
                        entries.forEach(([k, v], i) => {
                            const isFieldRef = typeof v === 'string' && (v.match(/^[a-z_][a-z0-9_.]*$/i) || v.includes('('));
                            const val = isFieldRef ? v : '\"' + v + '\"';
                            vpl += '        ' + k + ': ' + val + (i < entries.length - 1 ? ',' : '') + '\\n';
                        });
                        vpl += '    )\\n';
                    }
                    vpl += '\\n';
                });
            }
            
            return vpl;
        }
        
        function exportVPL() {
            const vpl = generateVPL();
            vscode.postMessage({ type: 'export', data: vpl });
        }
        
        function runFlow() {
            const vpl = generateVPL();
            vscode.postMessage({ type: 'run', data: vpl });
        }
        
        function saveFlow() {
            const flow = JSON.stringify({ nodes, connections }, null, 2);
            vscode.postMessage({ type: 'save', data: flow });
        }
        
        function clearAll() {
            if (confirm('Clear all nodes and connections?')) {
                nodes = [];
                connections = [];
                nodeId = 0;
                canvas.innerHTML = '';
                connSvg.innerHTML = '';
            }
        }
        
        function importVPL() {
            vscode.postMessage({ type: 'importVPL' });
        }
        
        function parseVPLToFlow(vplCode) {
            // Parse VPL code and generate flow nodes
            const newNodes = [];
            const newConnections = [];
            let yPos = 50;
            const xEvent = 100, xStream = 350, xPattern = 600;
            
            // Parse events
            const eventRegex = /event\\s+(\\w+):\\s*([\\s\\S]*?)(?=\\n(?:event|stream|fn|#|$))/g;
            let match;
            while ((match = eventRegex.exec(vplCode)) !== null) {
                const name = match[1];
                const fieldsBlock = match[2];
                const fields = fieldsBlock.trim().split('\\n').filter(l => l.trim() && !l.trim().startsWith('#')).map(l => l.trim()).join('\\n');
                newNodes.push({
                    id: 'node_' + (++nodeId),
                    type: 'event',
                    x: xEvent,
                    y: yPos,
                    label: name,
                    connector: null,
                    config: { fields }
                });
                yPos += 120;
            }
            
            yPos = 50;
            // Parse streams
            const streamRegex = /stream\\s+(\\w+)\\s*=\\s*(\\w+)([\\s\\S]*?)(?=\\n(?:stream|event|fn|#|$))/g;
            while ((match = streamRegex.exec(vplCode)) !== null) {
                const name = match[1];
                const source = match[2];
                const ops = match[3];
                
                const config = { source };
                
                // Parse operators
                const partitionMatch = ops.match(/\\.partition_by\\(([^)]+)\\)/);
                if (partitionMatch) config.partitionBy = partitionMatch[1];
                
                const windowMatch = ops.match(/\\.window\\(([^)]+)\\)/);
                if (windowMatch) config.window = windowMatch[1];
                
                const whereMatch = ops.match(/\\.where\\(([^)]+)\\)/);
                if (whereMatch) config.where = whereMatch[1];
                
                // Parse sequence patterns (->)
                const seqMatch = ops.match(/->\\s*\\w+/g);
                if (seqMatch) config.sequence = seqMatch.join(' ');
                
                // Parse attention_window
                const attMatch = ops.match(/\\.attention_window\\(([^)]+)\\)/);
                if (attMatch) {
                    const attArgs = attMatch[1];
                    const sizeM = attArgs.match(/size:\\s*(\\d+)/);
                    const patternM = attArgs.match(/pattern:\\s*(\\w+)/);
                    const threshM = attArgs.match(/threshold:\\s*([\\d.]+)/);
                    if (sizeM) config.attentionSize = sizeM[1];
                    if (patternM) config.attentionPattern = patternM[1];
                    if (threshM) config.attentionThreshold = threshM[1];
                }
                
                // Parse emit
                const emitMatch = ops.match(/\\.emit\\(([\\s\\S]*?)\\)/);
                if (emitMatch) {
                    try {
                        const emitContent = emitMatch[1].trim();
                        const emitObj = {};
                        emitContent.split(',').forEach(pair => {
                            const [k, v] = pair.split(':').map(s => s.trim());
                            if (k && v) emitObj[k] = v.replace(/"/g, '');
                        });
                        if (Object.keys(emitObj).length) config.emit = emitObj;
                    } catch(e) {}
                }
                
                newNodes.push({
                    id: 'node_' + (++nodeId),
                    type: 'stream',
                    x: xStream,
                    y: yPos,
                    label: name,
                    connector: null,
                    config
                });
                yPos += 150;
            }
            
            yPos = 50;
            // Parse functions
            const fnRegex = /fn\\s+(\\w+)\\(([^)]*)\\)\\s*->\\s*(\\w+):\\s*([\\s\\S]*?)(?=\\n(?:fn|stream|event|#|$))/g;
            while ((match = fnRegex.exec(vplCode)) !== null) {
                const name = match[1];
                const params = match[2];
                const returnType = match[3];
                const body = match[4].trim();
                
                newNodes.push({
                    id: 'node_' + (++nodeId),
                    type: 'function',
                    x: xPattern,
                    y: yPos,
                    label: name,
                    connector: null,
                    config: { params, returnType, body }
                });
                yPos += 120;
            }
            
            // Create connections between events and streams
            newNodes.filter(n => n.type === 'stream').forEach(stream => {
                const sourceEvent = newNodes.find(n => n.type === 'event' && n.label === stream.config.source);
                if (sourceEvent) {
                    newConnections.push({ from: sourceEvent.id, to: stream.id });
                }
            });
            
            return { nodes: newNodes, connections: newConnections };
        }
        
        // Handle messages from extension
        window.addEventListener('message', e => {
            if (e.data.type === 'load') {
                try {
                    const data = JSON.parse(e.data.data);
                    nodes = data.nodes || [];
                    connections = data.connections || [];
                    canvas.innerHTML = '';
                    nodes.forEach(n => renderNode(n));
                    renderConnections();
                } catch(err) {}
            }
            if (e.data.type === 'loadVPL') {
                try {
                    const result = parseVPLToFlow(e.data.data);
                    nodes = result.nodes;
                    connections = result.connections;
                    canvas.innerHTML = '';
                    nodes.forEach(n => renderNode(n));
                    renderConnections();
                } catch(err) {
                    console.error('Failed to parse VPL:', err);
                }
            }
        });
    </script>
</body>
</html>`;
}
