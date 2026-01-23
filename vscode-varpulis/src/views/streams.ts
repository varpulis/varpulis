import * as vscode from 'vscode';
import { StreamInfo } from '../engine';

export class StreamsTreeProvider implements vscode.TreeDataProvider<StreamItem> {
    private _onDidChangeTreeData = new vscode.EventEmitter<StreamItem | undefined>();
    readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

    private streams: StreamInfo[] = [];

    update(streams: StreamInfo[]): void {
        this.streams = streams;
        this._onDidChangeTreeData.fire(undefined);
    }

    refresh(): void {
        this._onDidChangeTreeData.fire(undefined);
    }

    getTreeItem(element: StreamItem): vscode.TreeItem {
        return element;
    }

    getChildren(element?: StreamItem): StreamItem[] {
        if (!element) {
            return this.streams.map(s => new StreamItem(s));
        }
        return [];
    }
}

class StreamItem extends vscode.TreeItem {
    constructor(public readonly stream: StreamInfo) {
        super(stream.name, vscode.TreeItemCollapsibleState.None);
        
        this.description = `${stream.eventsPerSecond.toFixed(1)}/s`;
        this.tooltip = `Source: ${stream.source}\nOperations: ${stream.operations.join(' → ')}\nStatus: ${stream.status}`;
        
        switch (stream.status) {
            case 'active':
                this.iconPath = new vscode.ThemeIcon('circle-filled', new vscode.ThemeColor('charts.green'));
                break;
            case 'paused':
                this.iconPath = new vscode.ThemeIcon('circle-outline', new vscode.ThemeColor('charts.yellow'));
                break;
            case 'error':
                this.iconPath = new vscode.ThemeIcon('error', new vscode.ThemeColor('charts.red'));
                break;
        }
    }
}
