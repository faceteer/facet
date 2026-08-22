import { describe, expect, test } from 'vitest';
import { condition, filter } from './condition.js';

interface User {
	id: string;
	age: number;
	name: string;
	favorites: string[];
	createdDate: Date;
	isActive?: boolean;
}

describe('condition.ts', () => {
	test('OR', () => {
		const compiled = condition<User>([
			['age', '>=', 21],
			'OR',
			['age', '<', 15],
		]);

		expect(compiled.expression).toBe('(#C_0_0 >= :C_0_0) OR (#C_1_0 < :C_1_0)');
		expect(compiled.values).toEqual({
			':C_0_0': { N: '21' },
			':C_1_0': { N: '15' },
		});

		expect(compiled.names).toEqual({
			'#C_0_0': 'age',
			'#C_1_0': 'age',
		});
	});

	test('Comparison With Date', () => {
		const compiled = condition<User>([
			'createdDate',
			'<=',
			new Date('2021-07-09T22:41:05.578Z'),
		]);

		expect(compiled.expression).toBe('#C_0 <= :C_0');
		expect(compiled.values).toEqual({
			':C_0': { S: '2021-07-09T22:41:05.578Z' },
		});

		expect(compiled.names).toEqual({
			'#C_0': 'createdDate',
		});
	});

	test('Begins With', () => {
		const compiled = condition<User>(['name', 'begins_with', 'larry']);

		expect(compiled.expression).toBe('begins_with (#C_0, :C_0)');
		expect(compiled.values).toEqual({
			':C_0': { S: 'larry' },
		});

		expect(compiled.names).toEqual({
			'#C_0': 'name',
		});
	});

	test('Contains', () => {
		const compiled = condition<User>(['favorites', 'contains', 'apples']);

		expect(compiled.expression).toBe('contains (#C_0, :C_0)');
		expect(compiled.values).toEqual({
			':C_0': { S: 'apples' },
		});

		expect(compiled.names).toEqual({
			'#C_0': 'favorites',
		});
	});

	test('Between', () => {
		const compiled = condition<User>(['age', 'between', 30, 39]);

		expect(compiled.expression).toBe('#C_0 BETWEEN :C_0_L AND :C_0_R');
		expect(compiled.values).toEqual({
			':C_0_L': { N: '30' },
			':C_0_R': { N: '39' },
		});

		expect(compiled.names).toEqual({
			'#C_0': 'age',
		});
	});

	test('Exists', () => {
		const compiled = condition<User>(['isActive', 'exists']);

		expect(compiled.expression).toBe('attribute_exists (#C_0)');
		expect(compiled.values).toEqual({});

		expect(compiled.names).toEqual({
			'#C_0': 'isActive',
		});
	});

	test('Not Exists', () => {
		const compiled = condition<User>(['isActive', 'not_exists']);

		expect(compiled.expression).toBe('attribute_not_exists (#C_0)');
		expect(compiled.values).toEqual({});

		expect(compiled.names).toEqual({
			'#C_0': 'isActive',
		});
	});

	test('NOT', () => {
		const compiled = condition<User>([
			['age', '>=', 21],
			'AND',
			{ NOT: ['createdDate', 'begins_with', '2021'] },
		]);

		expect(compiled.expression).toBe(
			'(#C_0_0 >= :C_0_0) AND (NOT (begins_with (#C_1_0_0, :C_1_0_0)))',
		);
		expect(compiled.values).toEqual({
			':C_0_0': {
				N: '21',
			},
			':C_1_0_0': {
				S: '2021',
			},
		});

		expect(compiled.names).toEqual({
			'#C_0_0': 'age',
			'#C_1_0_0': 'createdDate',
		});
	});

	test('Size', () => {
		const compiled = condition<User>(['name', 'size', '<=', 10]);

		expect(compiled.expression).toBe('size(#C_0) <= :C_0');
		expect(compiled.values).toEqual({
			':C_0': { N: '10' },
		});

		expect(compiled.names).toEqual({
			'#C_0': 'name',
		});
	});

	test('IN', () => {
		const compiled = condition<User>([
			'name',
			'in',
			['Dave', 'Larry', 'Mike', 'Jane'],
		]);

		expect(compiled.expression).toBe(
			'#C_0 IN (:C_0I0, :C_0I1, :C_0I2, :C_0I3)',
		);
		expect(compiled.values).toEqual({
			':C_0I0': {
				S: 'Dave',
			},
			':C_0I1': {
				S: 'Larry',
			},
			':C_0I2': {
				S: 'Mike',
			},
			':C_0I3': {
				S: 'Jane',
			},
		});

		expect(compiled.names).toEqual({
			'#C_0': 'name',
		});
	});

	test('throws on an empty in list', () => {
		expect(() => condition<User>(['name', 'in', []])).toThrow(
			"The 'in' operator requires at least one value",
		);
	});

	test('throws on an unknown operator', () => {
		expect(() => condition(['name', 'bogus', 10] as never)).toThrow(
			'Operator bogus is not defined',
		);
	});

	test('throws when a condition value has no DynamoDB representation', () => {
		expect(() =>
			condition<User>(['name', '=', undefined as unknown as string]),
		).toThrow('Unable to convert the value for the specified condition');
	});

	test('Filter Between', () => {
		const compiled = filter<User>(['age', 'between', 30, 39]);

		expect(compiled.expression).toBe('#F_0 BETWEEN :F_0_L AND :F_0_R');
		expect(compiled.values).toEqual({
			':F_0_L': { N: '30' },
			':F_0_R': { N: '39' },
		});

		expect(compiled.names).toEqual({
			'#F_0': 'age',
		});
	});
});
