import { Converter } from '@faceteer/converter';
import { AttributeValue } from '@faceteer/converter/attribute-value';

export interface CompiledExpression {
	names: { [key: string]: string };
	values: { [key: string]: AttributeValue };
	expression: string;
}

export type Comparator = '=' | '<>' | '<' | '<=' | '>' | '>=';

export type ComparatorCondition<T, U extends keyof T = any> = [
	U | string,
	Comparator,
	string | number | Date,
];
export type BetweenCondition<T, U extends keyof T = any> = [
	U | string,
	'between',
	string | number | Date,
	string | number | Date,
];
export type ExistsCondition<T> = [keyof T, 'exists'];
export type NotExistsCondition<T> = [keyof T, 'not_exists'];
export type BeginsWithCondition<T> = [keyof T, 'begins_with', string];
export type ContainsCondition<T, U extends keyof T = any> = [
	U | string,
	'contains',
	string | number | Date,
];
export type SizeCondition<T> = [keyof T, 'size', Comparator, number];

export type Condition<T> =
	| ComparatorCondition<T>
	| BetweenCondition<T>
	| ExistsCondition<T>
	| NotExistsCondition<T>
	| BeginsWithCondition<T>
	| ContainsCondition<T>;

export type LogicEvaluation<T> = [
	ConditionExpression<T>,
	'OR' | 'AND',
	ConditionExpression<T>,
];

export type ConditionExpression<T> =
	| Condition<T>
	| LogicEvaluation<T>
	| NotExpression<T>;
export type NotExpression<T> = { NOT: ConditionExpression<T> };

export function buildConditionExpression<T>(
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
		const notExpression = buildConditionExpression(
			expression.NOT,
			nextPrefix(),
		);
		notExpression.expression = `NOT (${notExpression.expression})`;
		return notExpression;
	}

	switch (expression[1]) {
		case 'OR':
		case 'AND': {
			const first = buildConditionExpression(expression[0], nextPrefix());
			Object.assign(compiledExpression.names, first.names);
			Object.assign(compiledExpression.values, first.values);
			const second = buildConditionExpression(expression[2], nextPrefix());
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
			compiledExpression.names[namePlaceholder] = expression[0];
			compiledExpression.values[valuePlaceholder] = Converter.input(
				expression[2],
			);
			compiledExpression.expression = `${namePlaceholder} ${expression[1]} ${valuePlaceholder}`;
			return compiledExpression;
		}
		case 'begins_with': {
			const placeholder = nextPrefix();
			const namePlaceholder = `#${placeholder}`;
			const valuePlaceholder = `:${placeholder}`;
			compiledExpression.names[namePlaceholder] = `${expression[0]}`;
			compiledExpression.values[valuePlaceholder] = Converter.input(
				expression[2],
			);
			compiledExpression.expression = `begins_with (${namePlaceholder}, ${valuePlaceholder})`;
			return compiledExpression;
		}
		case 'between': {
			const placeholder = nextPrefix();
			const namePlaceholder = `#${placeholder}`;
			const leftValuePlaceholder = `:${placeholder}_L`;
			const rightValuePlaceholder = `:${placeholder}_R`;
			compiledExpression.names[namePlaceholder] = expression[0];
			compiledExpression.values[leftValuePlaceholder] = Converter.input(
				expression[2],
			);
			compiledExpression.values[rightValuePlaceholder] = Converter.input(
				expression[3],
			);
			compiledExpression.expression = `${namePlaceholder} BETWEEN ${leftValuePlaceholder} AND ${rightValuePlaceholder}`;
			return compiledExpression;
		}
		case 'exists': {
			const placeholder = nextPrefix();
			const namePlaceholder = `#${placeholder}`;
			compiledExpression.names[namePlaceholder] = `${expression[0]}`;
			compiledExpression.expression = `attribute_exists (${namePlaceholder})`;
			return compiledExpression;
		}
		case 'not_exists': {
			const placeholder = nextPrefix();
			const namePlaceholder = `#${placeholder}`;
			compiledExpression.names[namePlaceholder] = `${expression[0]}`;
			compiledExpression.expression = `attribute_not_exists (${namePlaceholder})`;
			return compiledExpression;
		}

		default:
			throw new Error(`Operator ${expression[1]} is not defined`);
	}
}
