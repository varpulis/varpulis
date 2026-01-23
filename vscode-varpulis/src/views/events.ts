import * as vscode from 'vscode';
import { VarpulisEvent } from '../engine';

export class EventsTreeProvider implements vscode.TreeDataProvider<EventItem> {
    private _onDidChangeTreeData = new vscode.EventEmitter<EventItem | undefined>();
    readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

    private events: VarpulisEvent[] = [];
    private maxEvents = 100;

    addEvent(event: VarpulisEvent): void {
        this.events.unshift(event);
        if (this.events.length > this.maxEvents) {
            this.events.pop();
        }
        this._onDidChangeTreeData.fire(undefined);
    }

    clear(): void {
        this.events = [];
        this._onDidChangeTreeData.fire(undefined);
    }

    getTreeItem(element: EventItem): vscode.TreeItem {
        return element;
    }

    getChildren(): EventItem[] {
        return this.events.map(e => new EventItem(e));
    }
}

class EventItem extends vscode.TreeItem {
    constructor(public readonly event: VarpulisEvent) {
        super(event.type, vscode.TreeItemCollapsibleState.None);
        
        const time = new Date(event.timestamp).toLocaleTimeString();
        this.description = time;
        this.tooltip = JSON.stringify(event.data, null, 2);
        this.iconPath = new vscode.ThemeIcon('symbol-event');
    }
}
