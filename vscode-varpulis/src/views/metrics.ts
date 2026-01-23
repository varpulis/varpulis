import * as vscode from 'vscode';
import { EngineMetrics } from '../engine';

export class MetricsTreeProvider implements vscode.TreeDataProvider<MetricItem> {
    private _onDidChangeTreeData = new vscode.EventEmitter<MetricItem | undefined>();
    readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

    private metrics: EngineMetrics = {
        eventsProcessed: 0,
        alertsGenerated: 0,
        activeStreams: 0,
        uptime: 0,
        memoryUsage: 0,
        cpuUsage: 0
    };

    update(metrics: EngineMetrics): void {
        this.metrics = metrics;
        this._onDidChangeTreeData.fire(undefined);
    }

    getTreeItem(element: MetricItem): vscode.TreeItem {
        return element;
    }

    getChildren(): MetricItem[] {
        return [
            new MetricItem('Events Processed', this.formatNumber(this.metrics.eventsProcessed), 'symbol-event'),
            new MetricItem('Alerts Generated', this.formatNumber(this.metrics.alertsGenerated), 'bell'),
            new MetricItem('Active Streams', this.metrics.activeStreams.toString(), 'git-branch'),
            new MetricItem('Uptime', this.formatUptime(this.metrics.uptime), 'clock'),
            new MetricItem('Memory Usage', this.formatBytes(this.metrics.memoryUsage), 'database'),
            new MetricItem('CPU Usage', `${this.metrics.cpuUsage.toFixed(1)}%`, 'dashboard'),
        ];
    }

    private formatNumber(n: number): string {
        if (n >= 1000000) {
            return `${(n / 1000000).toFixed(1)}M`;
        }
        if (n >= 1000) {
            return `${(n / 1000).toFixed(1)}K`;
        }
        return n.toString();
    }

    private formatUptime(seconds: number): string {
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        const secs = Math.floor(seconds % 60);
        
        if (hours > 0) {
            return `${hours}h ${minutes}m`;
        }
        if (minutes > 0) {
            return `${minutes}m ${secs}s`;
        }
        return `${secs}s`;
    }

    private formatBytes(bytes: number): string {
        if (bytes >= 1073741824) {
            return `${(bytes / 1073741824).toFixed(1)} GB`;
        }
        if (bytes >= 1048576) {
            return `${(bytes / 1048576).toFixed(1)} MB`;
        }
        if (bytes >= 1024) {
            return `${(bytes / 1024).toFixed(1)} KB`;
        }
        return `${bytes} B`;
    }
}

class MetricItem extends vscode.TreeItem {
    constructor(label: string, value: string, icon: string) {
        super(label, vscode.TreeItemCollapsibleState.None);
        this.description = value;
        this.iconPath = new vscode.ThemeIcon(icon);
    }
}
