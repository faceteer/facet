export enum FilterComparison {
	Greater = '>',
	GreaterOrEqual = '>=',
	Less = '<',
	LessOrEqual = '<=',
	BeginsWith = 'begins_with',
}

export const FilterBetween = 'between';

export type CompareFilterCondition<T> = [
	keyof T,
	FilterComparison | `${FilterComparison}`,
	T[keyof T],
];

export type BetweenFilterCondition<T> = [
	keyof T,
	typeof FilterBetween,
	T[keyof T],
	T[keyof T],
];

export type FilterCondition<T> =
	| CompareFilterCondition<T>
	| BetweenFilterCondition<T>;
