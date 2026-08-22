import type { AttributeValue } from '@aws-sdk/client-dynamodb';
import { toAttributeValue } from '../converter/converter.js';

export type Comparator = '=' | '<>' | '<' | '<=' | '>' | '>=';

export interface CompiledExpression {
	names: Record<string, string>;
	values: Record<string, AttributeValue>;
	expression: string;
}

export type ComparatorCondition<T> = [
	keyof T | string,
	Comparator,
	string | number | Date | boolean,
];
export type BetweenCondition<T> = [
	keyof T | string,
	'between',
	string | number | Date | boolean,
	string | number | Date | boolean,
];
export type ExistsCondition<T> = [keyof T, 'exists'];
export type NotExistsCondition<T> = [keyof T, 'not_exists'];
export type BeginsWithCondition<T> = [keyof T, 'begins_with', string];
export type ContainsCondition<T> = [
	keyof T | string,
	'contains',
	string | number | Date | boolean,
];
export type SizeCondition<T> = [keyof T, 'size', Comparator, number];
export type InCondition<T> = [
	keyof T,
	'in',
	(string | number | Date | boolean)[],
];

export type Condition<T> =
	| ComparatorCondition<T>
	| BetweenCondition<T>
	| ExistsCondition<T>
	| NotExistsCondition<T>
	| BeginsWithCondition<T>
	| ContainsCondition<T>
	| SizeCondition<T>
	| InCondition<T>;

export type LogicEvaluation<T> = [
	ConditionExpression<T>,
	'OR' | 'AND',
	ConditionExpression<T>,
];

export type ConditionExpression<T> =
	Condition<T> | LogicEvaluation<T> | NotExpression<T>;
export interface NotExpression<T> {
	NOT: ConditionExpression<T>;
}

/**
 * Compile a condition expression tuple into a DynamoDB
 * `ConditionExpression` string with its attribute name and value
 * placeholder maps.
 *
 * @param expression An array representing a condition expression
 * @param prefix A prefix for expression attribute placeholders
 */
export function condition<T = unknown>(
	expression: ConditionExpression<T>,
	prefix = 'C',
): CompiledExpression {
	let attributeCounter = 0;

	/**
	 * To make sure all condition expression names
	 * and values don't collide we use a prefix before
	 * the placeholder in the string.
	 *
	 * This function will get the next placeholder and increment the counter
	 */
	const nextPrefix = () => {
		const hex = attributeCounter.toString(16);
		attributeCounter += 1;
		return `${prefix}_${hex}`;
	};

	const compiledExpression: CompiledExpression = {
		names: {},
		values: {},
		expression: '',
	};

	if (!Array.isArray(expression)) {
		const notExpression = condition(expression.NOT, nextPrefix());
		notExpression.expression = `NOT (${notExpression.expression})`;
		return notExpression;
	}

	switch (expression[1]) {
		case 'OR':
		case 'AND': {
			const first = condition(expression[0], nextPrefix());
			Object.assign(compiledExpression.names, first.names);
			Object.assign(compiledExpression.values, first.values);
			const second = condition(expression[2], nextPrefix());
			Object.assign(compiledExpression.names, second.names);
			Object.assign(compiledExpression.values, second.values);

			compiledExpression.expression = `(${first.expression}) ${expression[1]} (${second.expression})`;
			return compiledExpression;
		}

		case '=':
		case '<>':
		case '<':
		case '<=':
		case '>':
		case '>=': {
			const placeholder = nextPrefix();
			const namePlaceholder = `#${placeholder}`;
			const valuePlaceholder = `:${placeholder}`;
			compiledExpression.names[namePlaceholder] = String(expression[0]);
			compiledExpression.values[valuePlaceholder] = getValue(expression[2]);
			compiledExpression.expression = `${namePlaceholder} ${expression[1]} ${valuePlaceholder}`;
			return compiledExpression;
		}
		case 'begins_with': {
			const placeholder = nextPrefix();
			const namePlaceholder = `#${placeholder}`;
			const valuePlaceholder = `:${placeholder}`;
			compiledExpression.names[namePlaceholder] = String(expression[0]);
			compiledExpression.values[valuePlaceholder] = getValue(expression[2]);
			compiledExpression.expression = `begins_with (${namePlaceholder}, ${valuePlaceholder})`;
			return compiledExpression;
		}
		case 'contains': {
			const placeholder = nextPrefix();
			const namePlaceholder = `#${placeholder}`;
			const valuePlaceholder = `:${placeholder}`;
			compiledExpression.names[namePlaceholder] = String(expression[0]);
			compiledExpression.values[valuePlaceholder] = getValue(expression[2]);
			compiledExpression.expression = `contains (${namePlaceholder}, ${valuePlaceholder})`;
			return compiledExpression;
		}
		case 'between': {
			const placeholder = nextPrefix();
			const namePlaceholder = `#${placeholder}`;
			const leftValuePlaceholder = `:${placeholder}_L`;
			const rightValuePlaceholder = `:${placeholder}_R`;
			compiledExpression.names[namePlaceholder] = String(expression[0]);
			compiledExpression.values[leftValuePlaceholder] = getValue(expression[2]);
			compiledExpression.values[rightValuePlaceholder] = getValue(
				expression[3],
			);
			compiledExpression.expression = `${namePlaceholder} BETWEEN ${leftValuePlaceholder} AND ${rightValuePlaceholder}`;
			return compiledExpression;
		}
		case 'exists': {
			const placeholder = nextPrefix();
			const namePlaceholder = `#${placeholder}`;
			compiledExpression.names[namePlaceholder] = String(expression[0]);
			compiledExpression.expression = `attribute_exists (${namePlaceholder})`;
			return compiledExpression;
		}
		case 'not_exists': {
			const placeholder = nextPrefix();
			const namePlaceholder = `#${placeholder}`;
			compiledExpression.names[namePlaceholder] = String(expression[0]);
			compiledExpression.expression = `attribute_not_exists (${namePlaceholder})`;
			return compiledExpression;
		}
		case 'size': {
			const placeholder = nextPrefix();
			const namePlaceholder = `#${placeholder}`;
			const valuePlaceholder = `:${placeholder}`;
			compiledExpression.names[namePlaceholder] = String(expression[0]);
			compiledExpression.values[valuePlaceholder] = getValue(expression[3]);
			compiledExpression.expression = `size(${namePlaceholder}) ${expression[2]} ${valuePlaceholder}`;
			return compiledExpression;
		}
		case 'in': {
			if (expression[2].length === 0) {
				throw new Error(
					"The 'in' operator requires at least one value; DynamoDB rejects an empty IN list",
				);
			}
			const placeholder = nextPrefix();
			const namePlaceholder = `#${placeholder}`;
			compiledExpression.names[namePlaceholder] = String(expression[0]);
			const valuePlaceholders: string[] = [];
			for (const [index, listItem] of expression[2].entries()) {
				const valuePlaceholder = `:${placeholder}I${index}`;
				compiledExpression.values[valuePlaceholder] = getValue(listItem);
				valuePlaceholders.push(valuePlaceholder);
			}

			compiledExpression.expression = `${namePlaceholder} IN (${valuePlaceholders.join(
				', ',
			)})`;
			return compiledExpression;
		}

		default:
			throw new Error(`Operator ${String(expression[1])} is not defined`);
	}
}

function getValue(data: unknown): AttributeValue {
	const value = toAttributeValue(data);
	if (!value) {
		throw new Error('Unable to convert the value for the specified condition');
	}
	return value;
}

export type FilterCondition<T> =
	ComparatorCondition<T> | BetweenCondition<T> | BeginsWithCondition<T>;

export type FilterConditionExpression<T> =
	FilterCondition<T> | FilterLogicEvaluation<T>;

export type FilterLogicEvaluation<T> = [
	FilterConditionExpression<T>,
	'OR' | 'AND',
	FilterConditionExpression<T>,
];

/**
 * Build a filter expression for a DynamoDB query.
 *
 * See the {@link https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.FilterExpression|AWS Documentation}
 * for more information on filter expressions.
 *
 * @param expression An array representing a filter expression
 * @param prefix A prefix for expression attribute placeholders
 */
export function filter<T = unknown>(
	expression: FilterConditionExpression<T>,
	prefix = 'F',
): CompiledExpression {
	return condition(expression, prefix);
}
