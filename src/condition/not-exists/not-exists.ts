import type {
	ExpressionAttributeNameMap,
	ExpressionAttributeValueMap,
} from 'aws-sdk/clients/dynamodb';

import { BuiltCondition } from '../built-condition';

export type ConditionNotExistsOptions = {
	key: string;
};

export class ConditionNotExists {
	private readonly options: ConditionNotExistsOptions;

	constructor(options: ConditionNotExistsOptions) {
		this.options = options;
	}

	toString(): string {
		return `attribute_not_exists(${this.options.key})`;
	}

	build(prefix: string[]): BuiltCondition {
		const attributeNameKey = `#${[...prefix, this.options.key].join('_')}`;
		const attributeNames: ExpressionAttributeNameMap = {
			[attributeNameKey]: `${this.options.key}`,
		};
		const attributeValues: ExpressionAttributeValueMap = {};

		const statement = `attribute_not_exists (${attributeNameKey})`;
		return {
			attributeNames,
			attributeValues,
			statement,
		};
	}
}
