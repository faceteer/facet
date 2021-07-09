import type {
	ExpressionAttributeNameMap,
	ExpressionAttributeValueMap,
} from 'aws-sdk/clients/dynamodb';
import { Converter } from 'aws-sdk/clients/dynamodb';
import { ConditionValue } from '../condition-value';

import { BuiltCondition } from '../built-condition';

export type ConditionBeginsWithOptions = {
	key: string;
	value: ConditionValue;
};

export class ConditionBeginsWith {
	private readonly options: ConditionBeginsWithOptions;

	constructor(options: ConditionBeginsWithOptions) {
		this.options = options;
	}

	toString(): string {
		return `begins_with (${this.options.key}, ${this.options.key})`;
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

		const statement = `begins_with (${attributeNameKey}, ${attributeValueKey})`;
		return {
			attributeNames,
			attributeValues,
			statement,
		};
	}
}
