export enum FilterComparison {
	Greater = '>',
	GreaterOrEqual = '>=',
	Less = '<',
	LessOrEqual = '<=',
	Equals = '=',
	BeginsWith = 'begins_with',
}

export const FilterBetween = 'between';

export type CompareFilter<T> = [
	keyof T,
	FilterComparison | `${FilterComparison}`,
	string | number | Date,
];

export type BetweenFilter<T> = [
	keyof T,
	typeof FilterBetween,
	string | number | Date,
	string | number | Date,
];

export type Filter<T> = CompareFilter<T> | BetweenFilter<T>;
