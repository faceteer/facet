import type {
	ExpressionAttributeNameMap,
	ExpressionAttributeValueMap,
} from 'aws-sdk/clients/dynamodb';

export interface BuiltCondition {
	attributeNames: ExpressionAttributeNameMap;
	attributeValues: ExpressionAttributeValueMap;
	statement: string;
}
