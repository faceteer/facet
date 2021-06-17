export enum FilterComparison {
	Greater = '>',
	GreaterOrEqual = '>=',
	Less = '<',
	LessOrEqual = '<=',
	BeginsWith = 'begins_with',
}

export const FilterBetween = 'between';

export type CompareFilter<T> = [
	keyof T,
	FilterComparison | `${FilterComparison}`,
	T[keyof T],
];

export type BetweenFilter<T> = [
	keyof T,
	typeof FilterBetween,
	T[keyof T],
	T[keyof T],
];

export type Filter<T> = CompareFilter<T> | BetweenFilter<T>;
