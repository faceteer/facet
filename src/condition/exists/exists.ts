import type {
	ExpressionAttributeNameMap,
	ExpressionAttributeValueMap,
} from 'aws-sdk/clients/dynamodb';

import { BuiltCondition } from '../built-condition';

export type ConditionExistsOptions = {
	key: string;
};

export class ConditionExists {
	private readonly options: ConditionExistsOptions;

	constructor(options: ConditionExistsOptions) {
		this.options = options;
	}

	toString(): string {
		return `attribute_exists (${this.options.key})`;
	}

	build(prefix: string[]): BuiltCondition {
		const attributeNameKey = `#${[...prefix, this.options.key].join('_')}`;
		const attributeNames: ExpressionAttributeNameMap = {
			[attributeNameKey]: `${this.options.key}`,
		};
		const attributeValues: ExpressionAttributeValueMap = {};

		const statement = `attribute_exists (${attributeNameKey})`;
		return {
			attributeNames,
			attributeValues,
			statement,
		};
	}
}
