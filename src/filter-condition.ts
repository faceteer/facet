export enum FilterComparison {
	Greater = '>',
	GreaterOrEqual = '>=',
	Less = '<',
	LessOrEqual = '<=',
	BeginsWith = 'begins_with',
}

export type CompareFilterCondition<T, K extends keyof T> = [
	K,
	FilterComparison | `${FilterComparison}`,
	string | number,
];

export type BetweenFilterCondition<T, K extends keyof T> = [
	K,
	'between',
	string | number,
	string | number,
];

export type FilterCondition<T, K extends keyof T> =
	| CompareFilterCondition<T, K>
	| BetweenFilterCondition<T, K>;
