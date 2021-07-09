import type {
	ExpressionAttributeNameMap,
	ExpressionAttributeValueMap,
} from 'aws-sdk/clients/dynamodb';
import { Converter } from 'aws-sdk/clients/dynamodb';
import { ConditionValue } from '../condition-value';

import { BuiltCondition } from '../built-condition';

export type ConditionOperations = '=' | '<>' | '<' | '<=' | '>' | '>=';
export type ConditionComparisonOptions = {
	key: string;
	condition: ConditionOperations;
	value: ConditionValue;
};

export class ConditionComparison {
	private readonly options: ConditionComparisonOptions;

	constructor(options: ConditionComparisonOptions) {
		this.options = options;
	}

	toString(): string {
		return `${this.options.key} ${this.options.condition} ${this.options.value}`;
	}

	build(prefix: string[]): BuiltCondition {
		const attributeNameKey = `#${[...prefix, this.options.key].join('_')}`;
		const attributeValueKey = `:${[...prefix, this.options.key].join('_')}`;
		const attributeNames: ExpressionAttributeNameMap = {
			[attributeNameKey]: `${this.options.key}`,
		};
		const attributeValues: ExpressionAttributeValueMap = {
			[attributeValueKey]: Converter.input(this.options.value),
		};

		const statement = `${attributeNameKey} ${this.options.condition} ${attributeValueKey}`;
		return {
			attributeNames,
			attributeValues,
			statement,
		};
	}
}
