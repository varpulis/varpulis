import * as vscode from 'vscode';
import * as path from 'path';
import { VarpulisEngine } from './engine';
import { FlowEditorPanel, FlowEditorProvider } from './flowEditor';
import { AlertsTreeProvider } from './views/alerts';
import { EventsTreeProvider } from './views/events';
import { MetricsTreeProvider } from './views/metrics';
import { StreamsTreeProvider } from './views/streams';
import {
    LanguageClient,
    LanguageClientOptions,
    ServerOptions,
    TransportKind
} from 'vscode-languageclient/node';

let engine: VarpulisEngine | undefined;
let outputChannel: vscode.OutputChannel;
let statusBarItem: vscode.StatusBarItem;
let languageClient: LanguageClient | undefined;

export async function activate(context: vscode.ExtensionContext) {
    console.log('Varpulis extension activated');

    // Create output channel
    outputChannel = vscode.window.createOutputChannel('Varpulis');

    // Create status bar item
    statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 100);
    statusBarItem.text = '$(circle-outline) Varpulis';
    statusBarItem.tooltip = 'Varpulis Engine Status';
    statusBarItem.command = 'varpulis.showStatus';
    statusBarItem.show();
    context.subscriptions.push(statusBarItem);

    // Create tree view providers
    const streamsProvider = new StreamsTreeProvider();
    const eventsProvider = new EventsTreeProvider();
    const alertsProvider = new AlertsTreeProvider();
    const metricsProvider = new MetricsTreeProvider();

    // Register tree views
    vscode.window.registerTreeDataProvider('varpulis.streams', streamsProvider);
    vscode.window.registerTreeDataProvider('varpulis.events', eventsProvider);
    vscode.window.registerTreeDataProvider('varpulis.alerts', alertsProvider);
    vscode.window.registerTreeDataProvider('varpulis.metrics', metricsProvider);

    // Register Flow Editor
    context.subscriptions.push(FlowEditorProvider.register(context));
    context.subscriptions.push(
        vscode.commands.registerCommand('varpulis.openFlowEditor', () => {
            FlowEditorPanel.createOrShow(context.extensionUri);
        })
    );

    // Register commands
    context.subscriptions.push(
        vscode.commands.registerCommand('varpulis.startEngine', () => startEngine(streamsProvider, eventsProvider, alertsProvider, metricsProvider)),
        vscode.commands.registerCommand('varpulis.stopEngine', stopEngine),
        vscode.commands.registerCommand('varpulis.runFile', runCurrentFile),
        vscode.commands.registerCommand('varpulis.checkSyntax', checkSyntax),
        vscode.commands.registerCommand('varpulis.showMetrics', showMetrics),
        vscode.commands.registerCommand('varpulis.showStreams', () => streamsProvider.refresh()),
        vscode.commands.registerCommand('varpulis.injectEvent', injectEvent),
        vscode.commands.registerCommand('varpulis.injectEventFile', injectEventFile),
        vscode.commands.registerCommand('varpulis.showStatus', showStatus)
    );

    // Start Language Server if enabled
    const config = vscode.workspace.getConfiguration('varpulis');
    const enableLsp = config.get<boolean>('enableLsp', true);

    if (enableLsp) {
        try {
            await startLanguageServer(context);
            outputChannel.appendLine('Language Server started');
        } catch (error) {
            outputChannel.appendLine(`Failed to start Language Server: ${error}`);
            outputChannel.appendLine('Falling back to basic language features');
            registerFallbackProviders(context);
        }
    } else {
        registerFallbackProviders(context);
    }

    // Auto-start engine if configured
    if (config.get('autoStart')) {
        startEngine(streamsProvider, eventsProvider, alertsProvider, metricsProvider);
    }

    outputChannel.appendLine('Varpulis extension ready');
}

async function startLanguageServer(context: vscode.ExtensionContext) {
    const config = vscode.workspace.getConfiguration('varpulis');
    let serverPath = config.get<string>('lspServerPath', '');

    // If no path specified, try to find it
    if (!serverPath) {
        // First, check if bundled with extension
        const bundledPath = context.asAbsolutePath(path.join('bin', 'varpulis-lsp'));
        const fs = require('fs');
        if (fs.existsSync(bundledPath)) {
            serverPath = bundledPath;
        } else if (fs.existsSync(bundledPath + '.exe')) {
            serverPath = bundledPath + '.exe';
        } else {
            // Fall back to system PATH
            serverPath = 'varpulis-lsp';
        }
    }

    outputChannel.appendLine(`Starting LSP server: ${serverPath}`);

    const serverOptions: ServerOptions = {
        run: {
            command: serverPath,
            transport: TransportKind.stdio
        },
        debug: {
            command: serverPath,
            transport: TransportKind.stdio,
            options: {
                env: { ...process.env, RUST_LOG: 'varpulis_lsp=debug' }
            }
        }
    };

    const clientOptions: LanguageClientOptions = {
        documentSelector: [{ scheme: 'file', language: 'varpulis' }],
        synchronize: {
            fileEvents: vscode.workspace.createFileSystemWatcher('**/*.{vpl,varpulis}')
        },
        outputChannel: outputChannel
    };

    languageClient = new LanguageClient(
        'varpulis-lsp',
        'VPL Language Server',
        serverOptions,
        clientOptions
    );

    await languageClient.start();
    context.subscriptions.push(languageClient);
}

function registerFallbackProviders(context: vscode.ExtensionContext) {
    // Register diagnostics (fallback when LSP is not available)
    const diagnosticCollection = vscode.languages.createDiagnosticCollection('varpulis');
    context.subscriptions.push(diagnosticCollection);

    // Register document change listener for live validation
    context.subscriptions.push(
        vscode.workspace.onDidChangeTextDocument(event => {
            if (event.document.languageId === 'varpulis') {
                validateDocument(event.document, diagnosticCollection);
            }
        })
    );

    // Register document open listener
    context.subscriptions.push(
        vscode.workspace.onDidOpenTextDocument(document => {
            if (document.languageId === 'varpulis') {
                validateDocument(document, diagnosticCollection);
            }
        })
    );

    // Register completion provider
    context.subscriptions.push(
        vscode.languages.registerCompletionItemProvider('varpulis', {
            provideCompletionItems: provideCompletionItems
        }, '.', '(')
    );

    // Register hover provider
    context.subscriptions.push(
        vscode.languages.registerHoverProvider('varpulis', {
            provideHover: provideHover
        })
    );
}

export async function deactivate(): Promise<void> {
    if (languageClient) {
        await languageClient.stop();
    }
    if (engine) {
        engine.stop();
    }
}

async function startEngine(
    streamsProvider: StreamsTreeProvider,
    eventsProvider: EventsTreeProvider,
    alertsProvider: AlertsTreeProvider,
    metricsProvider: MetricsTreeProvider
) {
    if (engine && engine.isRunning()) {
        vscode.window.showInformationMessage('Varpulis engine is already running');
        return;
    }

    const config = vscode.workspace.getConfiguration('varpulis');
    const enginePath = config.get<string>('enginePath') || 'varpulis';
    const port = config.get<number>('enginePort') || 9000;

    try {
        engine = new VarpulisEngine(enginePath, port, outputChannel);
        
        // Set up event handlers
        engine.onStreamUpdate(streams => streamsProvider.update(streams));
        engine.onEvent(event => eventsProvider.addEvent(event));
        engine.onAlert(alert => alertsProvider.addAlert(alert));
        engine.onMetrics(metrics => metricsProvider.update(metrics));
        
        await engine.start();
        
        statusBarItem.text = '$(circle-filled) Varpulis';
        statusBarItem.backgroundColor = new vscode.ThemeColor('statusBarItem.warningBackground');
        
        vscode.window.showInformationMessage('Varpulis engine started');
        outputChannel.appendLine('Engine started on port ' + port);
    } catch (error) {
        vscode.window.showErrorMessage(`Failed to start Varpulis engine: ${error}`);
        outputChannel.appendLine(`Error: ${error}`);
    }
}

async function stopEngine() {
    if (!engine || !engine.isRunning()) {
        vscode.window.showInformationMessage('Varpulis engine is not running');
        return;
    }

    try {
        await engine.stop();
        statusBarItem.text = '$(circle-outline) Varpulis';
        statusBarItem.backgroundColor = undefined;
        vscode.window.showInformationMessage('Varpulis engine stopped');
        outputChannel.appendLine('Engine stopped');
    } catch (error) {
        vscode.window.showErrorMessage(`Failed to stop Varpulis engine: ${error}`);
    }
}

async function runCurrentFile() {
    const editor = vscode.window.activeTextEditor;
    if (!editor || editor.document.languageId !== 'varpulis') {
        vscode.window.showErrorMessage('No VPL file is open');
        return;
    }

    const document = editor.document;
    
    // Save the file first
    await document.save();

    if (!engine || !engine.isRunning()) {
        const start = await vscode.window.showQuickPick(['Yes', 'No'], {
            placeHolder: 'Varpulis engine is not running. Start it?'
        });
        if (start === 'Yes') {
            await vscode.commands.executeCommand('varpulis.startEngine');
        } else {
            return;
        }
    }

    try {
        outputChannel.appendLine(`Running: ${document.fileName}`);
        outputChannel.show();
        
        const result = await engine!.loadFile(document.fileName);
        
        if (result.success) {
            vscode.window.showInformationMessage(`Loaded ${result.streamsLoaded} streams`);
            outputChannel.appendLine(`Success: Loaded ${result.streamsLoaded} streams`);
        } else {
            vscode.window.showErrorMessage(`Failed to load: ${result.error}`);
            outputChannel.appendLine(`Error: ${result.error}`);
        }
    } catch (error) {
        vscode.window.showErrorMessage(`Error running file: ${error}`);
        outputChannel.appendLine(`Error: ${error}`);
    }
}

async function checkSyntax() {
    const editor = vscode.window.activeTextEditor;
    if (!editor || editor.document.languageId !== 'varpulis') {
        vscode.window.showErrorMessage('No VPL file is open');
        return;
    }

    const document = editor.document;
    const config = vscode.workspace.getConfiguration('varpulis');
    const enginePath = config.get<string>('enginePath') || 'varpulis';

    try {
        const { exec } = require('child_process');
        const util = require('util');
        const execPromise = util.promisify(exec);

        await document.save();
        
        const { stdout, stderr } = await execPromise(`${enginePath} check "${document.fileName}"`);
        
        if (stderr) {
            vscode.window.showErrorMessage(`Syntax error: ${stderr}`);
            outputChannel.appendLine(`Syntax error: ${stderr}`);
        } else {
            vscode.window.showInformationMessage('Syntax OK ✓');
            outputChannel.appendLine(`Syntax check passed: ${document.fileName}`);
        }
    } catch (error: any) {
        vscode.window.showErrorMessage(`Syntax error: ${error.message}`);
        outputChannel.appendLine(`Syntax error: ${error.message}`);
    }
}

async function showMetrics() {
    const config = vscode.workspace.getConfiguration('varpulis');
    const metricsPort = config.get<number>('metricsPort') || 9090;
    const metricsUrl = `http://localhost:${metricsPort}/metrics`;

    // Open in browser or show in panel
    const choice = await vscode.window.showQuickPick(
        ['Open in Browser', 'Show in Panel'],
        { placeHolder: 'How would you like to view metrics?' }
    );

    if (choice === 'Open in Browser') {
        vscode.env.openExternal(vscode.Uri.parse(metricsUrl));
    } else if (choice === 'Show in Panel') {
        // Create webview panel
        const panel = vscode.window.createWebviewPanel(
            'varpulisMetrics',
            'Varpulis Metrics',
            vscode.ViewColumn.Beside,
            { enableScripts: true }
        );
        
        panel.webview.html = getMetricsHtml(metricsUrl);
    }
}

async function injectEvent() {
    if (!engine || !engine.isRunning()) {
        vscode.window.showErrorMessage('Varpulis engine is not running');
        return;
    }

    // Get event type
    const eventType = await vscode.window.showInputBox({
        prompt: 'Enter event type (e.g., TemperatureReading)',
        placeHolder: 'EventType'
    });

    if (!eventType) {
        return;
    }

    // Get event data as JSON
    const eventData = await vscode.window.showInputBox({
        prompt: 'Enter event data as JSON',
        placeHolder: '{"sensor_id": "s1", "value": 22.5}'
    });

    if (!eventData) {
        return;
    }

    try {
        const data = JSON.parse(eventData);
        await engine.injectEvent(eventType, data);
        vscode.window.showInformationMessage(`Injected ${eventType} event`);
        outputChannel.appendLine(`Injected event: ${eventType} - ${eventData}`);
    } catch (error) {
        vscode.window.showErrorMessage(`Failed to inject event: ${error}`);
    }
}

async function injectEventFile() {
    if (!engine || !engine.isRunning()) {
        vscode.window.showErrorMessage('Varpulis engine is not running');
        return;
    }

    // Let user pick an .evt file
    const files = await vscode.window.showOpenDialog({
        canSelectFiles: true,
        canSelectMany: false,
        filters: { 'Event Files': ['evt'], 'All Files': ['*'] },
        title: 'Select Event File (.evt)'
    });

    if (!files || files.length === 0) {
        return;
    }

    const filePath = files[0].fsPath;

    // Ask for mode
    const mode = await vscode.window.showQuickPick(
        [
            { label: 'Timed', description: 'Respect BATCH timing delays', value: false },
            { label: 'Immediate', description: 'Send all events immediately', value: true }
        ],
        { placeHolder: 'Select injection mode' }
    );

    if (!mode) {
        return;
    }

    try {
        outputChannel.appendLine(`Injecting events from: ${filePath}`);
        outputChannel.appendLine(`Mode: ${mode.label}`);
        outputChannel.show();

        // Show progress
        await vscode.window.withProgress({
            location: vscode.ProgressLocation.Notification,
            title: 'Injecting events',
            cancellable: false
        }, async (progress) => {
            const result = await engine!.injectEventFile(
                filePath,
                mode.value,
                (current, total, eventType) => {
                    const pct = Math.round((current / total) * 100);
                    progress.report({ 
                        message: `${current}/${total} - ${eventType}`,
                        increment: (1 / total) * 100
                    });
                    outputChannel.appendLine(`  [${current}/${total}] ${eventType}`);
                }
            );

            if (result.errors.length > 0) {
                vscode.window.showWarningMessage(
                    `Injected ${result.sent} events with ${result.errors.length} errors`
                );
                result.errors.forEach(e => outputChannel.appendLine(`  Error: ${e}`));
            } else {
                vscode.window.showInformationMessage(`Successfully injected ${result.sent} events`);
            }

            outputChannel.appendLine(`Injection complete: ${result.sent} events sent`);
        });
    } catch (error) {
        vscode.window.showErrorMessage(`Failed to inject event file: ${error}`);
        outputChannel.appendLine(`Error: ${error}`);
    }
}

function showStatus() {
    if (!engine) {
        vscode.window.showInformationMessage('Varpulis engine: Not initialized');
        return;
    }

    const status = engine.getStatus();
    const message = `Varpulis Engine
Status: ${status.running ? 'Running' : 'Stopped'}
Streams: ${status.activeStreams}
Events processed: ${status.eventsProcessed}
Alerts generated: ${status.alertsGenerated}`;

    vscode.window.showInformationMessage(message);
}

function validateDocument(document: vscode.TextDocument, diagnostics: vscode.DiagnosticCollection) {
    const text = document.getText();
    const problems: vscode.Diagnostic[] = [];

    // Basic validation rules
    const lines = text.split('\n');
    
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        
        // Check for common issues
        if (line.includes('strean ')) {
            const index = line.indexOf('strean ');
            problems.push(new vscode.Diagnostic(
                new vscode.Range(i, index, i, index + 6),
                'Did you mean "stream"?',
                vscode.DiagnosticSeverity.Error
            ));
        }
        
        // Check for unclosed strings
        const doubleQuotes = (line.match(/"/g) || []).length;
        if (doubleQuotes % 2 !== 0 && !line.trim().startsWith('#')) {
            problems.push(new vscode.Diagnostic(
                new vscode.Range(i, 0, i, line.length),
                'Unclosed string',
                vscode.DiagnosticSeverity.Error
            ));
        }
    }

    diagnostics.set(document.uri, problems);
}

function provideCompletionItems(
    document: vscode.TextDocument,
    position: vscode.Position
): vscode.CompletionItem[] {
    const linePrefix = document.lineAt(position).text.substring(0, position.character);
    const items: vscode.CompletionItem[] = [];

    // After a dot, suggest stream operations
    if (linePrefix.endsWith('.')) {
        const streamOps = [
            { label: 'where', detail: 'Filter events', insertText: 'where($1)' },
            { label: 'select', detail: 'Project fields', insertText: 'select(\n    $1\n)' },
            { label: 'aggregate', detail: 'Aggregate values', insertText: 'aggregate(\n    $1\n)' },
            { label: 'window', detail: 'Temporal window', insertText: 'window($1)' },
            { label: 'partition_by', detail: 'Partition stream', insertText: 'partition_by($1)' },
            { label: 'emit', detail: 'Emit to sink', insertText: 'emit(\n    $1\n)' },
            { label: 'pattern', detail: 'Pattern matching', insertText: 'pattern(\n    $1: events => $2\n)' },
            { label: 'to', detail: 'Output destination', insertText: 'to("$1")' },
            { label: 'forecast', detail: 'PST pattern forecasting', insertText: 'forecast(confidence: ${1:0.7}, horizon: ${2:2m})' },
        ];

        for (const op of streamOps) {
            const item = new vscode.CompletionItem(op.label, vscode.CompletionItemKind.Method);
            item.detail = op.detail;
            item.insertText = new vscode.SnippetString(op.insertText);
            items.push(item);
        }
    }

    // Suggest keywords
    const keywords = ['stream', 'event', 'from', 'let', 'const', 'fn', 'config', 'if', 'else', 'for', 'while', 'match'];
    for (const kw of keywords) {
        if (linePrefix.trim() === '' || kw.startsWith(linePrefix.trim())) {
            const item = new vscode.CompletionItem(kw, vscode.CompletionItemKind.Keyword);
            items.push(item);
        }
    }

    // Suggest aggregation functions
    if (linePrefix.includes('aggregate') || linePrefix.includes('select')) {
        const aggFuncs = ['sum', 'avg', 'count', 'min', 'max', 'first', 'last', 'stddev', 'variance'];
        for (const fn of aggFuncs) {
            const item = new vscode.CompletionItem(fn, vscode.CompletionItemKind.Function);
            item.insertText = new vscode.SnippetString(`${fn}($1)`);
            items.push(item);
        }
    }

    return items;
}

function provideHover(
    document: vscode.TextDocument,
    position: vscode.Position
): vscode.Hover | undefined {
    const range = document.getWordRangeAtPosition(position);
    if (!range) {
        return undefined;
    }

    const word = document.getText(range);
    
    const docs: { [key: string]: string } = {
        'stream': '**stream** - Declares a new event stream\n\n```varpulis\nstream Name = EventType\nstream Name = OtherStream.where(...)\n```',
        'event': '**event** - Declares an event type\n\n```varpulis\nevent MyEvent:\n    field1: string\n    field2: float\n```',
        'where': '**.where(condition)** - Filters events based on a condition\n\n```varpulis\n.where(value > 10)\n.where(status == "active" and count > 0)\n```',
        'window': '**.window(duration)** - Creates a temporal window\n\n```varpulis\n.window(5m)              # Tumbling window\n.window(5m, sliding: 1m) # Sliding window\n```',
        'aggregate': '**.aggregate(...)** - Aggregates values in a window\n\n```varpulis\n.aggregate(\n    total: sum(value),\n    average: avg(value),\n    count: count()\n)\n```',
        'pattern': '**.pattern(...)** - Defines a pattern to detect\n\n```varpulis\n.pattern(\n    my_pattern: events =>\n        events.filter(...).count() > threshold\n)\n```',
        'emit': '**.emit(...)** - Emits an alert or output event\n\n```varpulis\n.emit(\n    alert_type: "anomaly",\n    severity: "warning"\n)\n```',
        'forecast': '**.forecast(...)** - PST-based pattern forecasting\n\nPredicts whether a partially-matched sequence will complete.\n\n```varpulis\n.forecast(confidence: 0.7, horizon: 2m, warmup: 500)\n```\n\n**Built-in variables:** `forecast_probability`, `forecast_time`, `forecast_state`',
    };

    if (docs[word]) {
        return new vscode.Hover(new vscode.MarkdownString(docs[word]));
    }

    return undefined;
}

function getMetricsHtml(metricsUrl: string): string {
    return `<!DOCTYPE html>
<html>
<head>
    <style>
        body { font-family: var(--vscode-font-family); padding: 10px; }
        pre { background: var(--vscode-editor-background); padding: 10px; overflow: auto; }
        .refresh { margin-bottom: 10px; }
        button { padding: 5px 10px; cursor: pointer; }
    </style>
</head>
<body>
    <div class="refresh">
        <button onclick="refresh()">Refresh</button>
        <span id="lastUpdate"></span>
    </div>
    <pre id="metrics">Loading...</pre>
    <script>
        async function refresh() {
            try {
                const response = await fetch('${metricsUrl}');
                const text = await response.text();
                document.getElementById('metrics').textContent = text;
                document.getElementById('lastUpdate').textContent = 'Last update: ' + new Date().toLocaleTimeString();
            } catch (error) {
                document.getElementById('metrics').textContent = 'Error: ' + error.message;
            }
        }
        refresh();
        setInterval(refresh, 5000);
    </script>
</body>
</html>`;
}
