import type { AttributeValue } from '@aws-sdk/client-dynamodb';
import {
	condition,
	type ConditionExpression,
} from '@faceteer/expression-builder';

/**
 * Subset of PutItemInput / DeleteItemInput that holds an optional
 * condition expression plus its name/value maps.
 *
 * DynamoDB rejects requests where `ExpressionAttributeNames` or
 * `ExpressionAttributeValues` is present but empty, so the helper
 * only sets them when the compiled expression actually produced
 * entries.
 */
export interface ConditionalInput {
	ConditionExpression?: string;
	ExpressionAttributeNames?: Record<string, string>;
	ExpressionAttributeValues?: Record<string, AttributeValue>;
}

export function applyCondition<T>(
	input: ConditionalInput,
	expression: ConditionExpression<T>,
): void {
	const compiled = condition(expression);
	input.ConditionExpression = compiled.expression;
	if (Object.keys(compiled.names).length > 0) {
		input.ExpressionAttributeNames = compiled.names;
	}
	if (Object.keys(compiled.values).length > 0) {
		input.ExpressionAttributeValues = compiled.values;
	}
}
