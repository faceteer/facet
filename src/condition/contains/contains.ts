import type {
	ExpressionAttributeNameMap,
	ExpressionAttributeValueMap,
} from 'aws-sdk/clients/dynamodb';
import { Converter } from 'aws-sdk/clients/dynamodb';
import { ConditionValue } from '../condition-value';

import { BuiltCondition } from '../built-condition';

export type ConditionContainsOptions = {
	key: string;
	value: ConditionValue;
};

export class ConditionContains {
	private readonly options: ConditionContainsOptions;

	constructor(options: ConditionContainsOptions) {
		this.options = options;
	}

	toString(): string {
		return `contains (${this.options.key}, ${this.options.value})`;
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

		const statement = `contains (${attributeNameKey}, ${attributeValueKey})`;
		return {
			attributeNames,
			attributeValues,
			statement,
		};
	}
}
