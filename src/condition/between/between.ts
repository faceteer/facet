import type {
	ExpressionAttributeNameMap,
	ExpressionAttributeValueMap,
} from 'aws-sdk/clients/dynamodb';
import { Converter } from 'aws-sdk/clients/dynamodb';
import { ConditionValue } from '../condition-value';

import { BuiltCondition } from '../built-condition';

export type ConditionBetweenOptions = {
	key: string;
	start: ConditionValue;
	end: ConditionValue;
};

export class ConditionBetween {
	private readonly options: ConditionBetweenOptions;

	constructor(options: ConditionBetweenOptions) {
		this.options = options;
	}

	toString(): string {
		return `${this.options.key} BETWEEN ${this.options.start} AND ${this.options.end}`;
	}

	build(prefix: string[]): BuiltCondition {
		const attributeNameKey = `#${[...prefix, this.options.key].join('_')}`;
		const attributeValueStartKey = `:${[
			...prefix,
			this.options.key,
			'start',
		].join('_')}`;
		const attributeValueEndKey = `:${[...prefix, this.options.key, 'end'].join(
			'_',
		)}`;
		const attributeNames: ExpressionAttributeNameMap = {
			[attributeNameKey]: `${this.options.key}`,
		};
		const attributeValues: ExpressionAttributeValueMap = {
			[attributeValueStartKey]: Converter.input(this.options.start),
			[attributeValueEndKey]: Converter.input(this.options.end),
		};

		const statement = `${attributeNameKey} BETWEEN ${attributeValueStartKey} AND ${attributeValueEndKey}`;
		return {
			attributeNames,
			attributeValues,
			statement,
		};
	}
}
