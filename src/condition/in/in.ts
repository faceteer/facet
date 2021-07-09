import type {
	ExpressionAttributeNameMap,
	ExpressionAttributeValueMap,
} from 'aws-sdk/clients/dynamodb';
import { Converter } from 'aws-sdk/clients/dynamodb';
import { ConditionValue } from '../condition-value';

import { BuiltCondition } from '../built-condition';

export type ConditionInOptions = {
	key: string;
	in: ConditionValue[];
};

export class ConditionIn {
	private readonly options: ConditionInOptions;

	constructor(options: ConditionInOptions) {
		this.options = options;
	}

	toString(): string {
		return `${this.options.key} IN (${this.options.in.join(', ')})`;
	}

	build(prefix: string[]): BuiltCondition {
		const attributeNameKey = `#${[...prefix, this.options.key].join('_')}`;
		const attributeValueKeys: string[] = [];
		const attributeNames: ExpressionAttributeNameMap = {
			[attributeNameKey]: `${this.options.key}`,
		};
		const attributeValues: ExpressionAttributeValueMap = {};

		for (const [index, inItem] of this.options.in.entries()) {
			const attributeValueKey = `:${[...prefix, index, this.options.key].join(
				'_',
			)}`;
			attributeValueKeys.push(attributeValueKey);
			attributeValues[attributeValueKey] = Converter.input(inItem);
		}

		const statement = `${attributeNameKey} IN (${attributeValueKeys.join(
			', ',
		)})`;

		return {
			attributeNames,
			attributeValues,
			statement,
		};
	}
}
