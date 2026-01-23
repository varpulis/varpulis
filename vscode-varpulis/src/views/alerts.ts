import * as vscode from 'vscode';
import { VarpulisAlert } from '../engine';

export class AlertsTreeProvider implements vscode.TreeDataProvider<AlertItem> {
    private _onDidChangeTreeData = new vscode.EventEmitter<AlertItem | undefined>();
    readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

    private alerts: VarpulisAlert[] = [];
    private maxAlerts = 50;

    addAlert(alert: VarpulisAlert): void {
        this.alerts.unshift(alert);
        if (this.alerts.length > this.maxAlerts) {
            this.alerts.pop();
        }
        this._onDidChangeTreeData.fire(undefined);

        // Show notification for critical alerts
        if (alert.severity === 'critical') {
            vscode.window.showWarningMessage(`🚨 ${alert.type}: ${alert.message}`);
        }
    }

    clear(): void {
        this.alerts = [];
        this._onDidChangeTreeData.fire(undefined);
    }

    getTreeItem(element: AlertItem): vscode.TreeItem {
        return element;
    }

    getChildren(): AlertItem[] {
        return this.alerts.map(a => new AlertItem(a));
    }
}

class AlertItem extends vscode.TreeItem {
    constructor(public readonly alert: VarpulisAlert) {
        super(alert.type, vscode.TreeItemCollapsibleState.None);
        
        const time = new Date(alert.timestamp).toLocaleTimeString();
        this.description = `${alert.severity} - ${time}`;
        this.tooltip = `${alert.message}\n\n${JSON.stringify(alert.data, null, 2)}`;
        
        switch (alert.severity) {
            case 'critical':
                this.iconPath = new vscode.ThemeIcon('error', new vscode.ThemeColor('errorForeground'));
                break;
            case 'warning':
                this.iconPath = new vscode.ThemeIcon('warning', new vscode.ThemeColor('editorWarning.foreground'));
                break;
            case 'info':
                this.iconPath = new vscode.ThemeIcon('info', new vscode.ThemeColor('editorInfo.foreground'));
                break;
        }
    }
}
