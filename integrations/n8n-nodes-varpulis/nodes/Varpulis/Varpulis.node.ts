import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
} from 'n8n-workflow';

interface EngineOutput {
	stream: string;
	event: Record<string, unknown>;
	timestamp?: string;
}

interface ProcessResult {
	ok: boolean;
	outputs?: EngineOutput[];
	error?: string;
}

interface LoadResult {
	ok: boolean;
	streams?: string[];
	error?: string;
}

export class Varpulis implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Varpulis CEP',
		name: 'varpulis',
		icon: 'fa:bolt',
		group: ['transform'],
		version: 1,
		subtitle: '={{$parameter["eventType"]}} pattern detection',
		description: 'Temporal pattern detection powered by the Varpulis CEP engine (WASM)',
		defaults: {
			name: 'Varpulis CEP',
			color: '#FF6600',
		},
		inputs: ['main'],
		outputs: ['main', 'main'],
		outputNames: ['Matches', 'Passthrough'],
		properties: [
			{
				displayName: 'VPL Pattern',
				name: 'vplPattern',
				type: 'string',
				typeOptions: {
					rows: 12,
				},
				default:
					'event Trade:\n    symbol: string\n    price: float\n    volume: int\n\nstream LargeTradeSpike =\n    Trade as a -> Trade as b\n    .where(a.volume > 10000 AND b.price > a.price * 1.05)\n    .within(1m)\n    .emit(symbol: a.symbol, spike_pct: (b.price - a.price) / a.price * 100)',
				placeholder: 'Enter your VPL pattern definition...',
				description:
					'The Varpulis Pattern Language definition. Declare event types and stream patterns. See https://www.varpulis-cep.com/docs/syntax for full syntax reference.',
				required: true,
				noDataExpression: true,
			},
			{
				displayName: 'Event Type',
				name: 'eventType',
				type: 'string',
				default: '',
				placeholder: 'e.g. Trade',
				description:
					'The event type name to tag incoming items with. Must match an event declaration in the VPL pattern.',
				required: true,
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const matchOutput: INodeExecutionData[] = [];
		const passthroughOutput: INodeExecutionData[] = [];

		const vplPattern = this.getNodeParameter('vplPattern', 0) as string;
		const eventType = this.getNodeParameter('eventType', 0) as string;

		if (!vplPattern.trim()) {
			throw new NodeOperationError(this.getNode(), 'VPL Pattern cannot be empty');
		}
		if (!eventType.trim()) {
			throw new NodeOperationError(this.getNode(), 'Event Type cannot be empty');
		}

		// Load the WASM engine
		let WasmEngine: { new (): {
			load(vpl: string): string;
			processEvent(eventJson: string): string;
			free(): void;
		}};

		try {
			const wasmModule = require('@varpulis/engine');
			WasmEngine = wasmModule.WasmEngine;
		} catch (err) {
			throw new NodeOperationError(
				this.getNode(),
				'Failed to load Varpulis WASM engine. Ensure the @varpulis/engine package is installed correctly.',
				{ description: String(err) },
			);
		}

		const engine = new WasmEngine();

		try {
			// Load the VPL program
			const loadResultJson = engine.load(vplPattern);
			let loadResult: LoadResult;
			try {
				loadResult = JSON.parse(loadResultJson);
			} catch {
				throw new NodeOperationError(
					this.getNode(),
					`Engine returned invalid JSON from load(): ${loadResultJson}`,
				);
			}

			if (!loadResult.ok) {
				throw new NodeOperationError(
					this.getNode(),
					`VPL compilation failed: ${loadResult.error ?? 'unknown error'}`,
				);
			}

			// Process each input item
			for (let i = 0; i < items.length; i++) {
				const item = items[i];

				// Always pass through the original item
				passthroughOutput.push(item);

				// Build the event JSON from the item data
				const eventData: Record<string, unknown> = {
					event_type: eventType,
					...item.json,
				};

				let result: ProcessResult;
				try {
					const resultJson = engine.processEvent(JSON.stringify(eventData));
					result = JSON.parse(resultJson);
				} catch (err) {
					throw new NodeOperationError(
						this.getNode(),
						`Failed to process event at item ${i}: ${String(err)}`,
						{ itemIndex: i },
					);
				}

				if (!result.ok) {
					throw new NodeOperationError(
						this.getNode(),
						`Engine error at item ${i}: ${result.error ?? 'unknown error'}`,
						{ itemIndex: i },
					);
				}

				// Emit each output as a separate match item
				if (result.outputs && result.outputs.length > 0) {
					for (const output of result.outputs) {
						matchOutput.push({
							json: {
								_stream: output.stream,
								_timestamp: output.timestamp,
								_sourceIndex: i,
								...output.event,
							},
						});
					}
				}
			}
		} finally {
			// Free WASM memory
			try {
				engine.free();
			} catch {
				// Ignore cleanup errors
			}
		}

		return [matchOutput, passthroughOutput];
	}
}
