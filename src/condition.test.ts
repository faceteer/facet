import { buildConditionExpression } from './condition';

interface User {
	id: string;
	age: number;
	name: string;
	createdDate: Date;
	isActive?: boolean;
}

describe('condition.ts', () => {
	test('OR', () => {
		const conditionExpression = buildConditionExpression<User>([
			['age', '>=', 21],
			'OR',
			['age', '<', 15],
		]);

		expect(conditionExpression.expression).toBe(
			'(#C_0_0 >= :C_0_0) OR (#C_1_0 < :C_1_0)',
		);
		expect(conditionExpression.values).toEqual({
			':C_0_0': { N: '21' },
			':C_1_0': { N: '15' },
		});

		expect(conditionExpression.names).toEqual({
			'#C_0_0': 'age',
			'#C_1_0': 'age',
		});
	});

	test('Comparison With Date', () => {
		const conditionExpression = buildConditionExpression<User>([
			'createdDate',
			'<=',
			new Date('2021-07-09T22:41:05.578Z'),
		]);

		expect(conditionExpression.expression).toBe('#C_0 <= :C_0');
		expect(conditionExpression.values).toEqual({
			':C_0': { S: '2021-07-09T22:41:05.578Z' },
		});

		expect(conditionExpression.names).toEqual({
			'#C_0': 'createdDate',
		});
	});

	test('Begins With', () => {
		const conditionExpression = buildConditionExpression<User>([
			'name',
			'begins_with',
			'larry',
		]);

		expect(conditionExpression.expression).toBe('begins_with (#C_0, :C_0)');
		expect(conditionExpression.values).toEqual({
			':C_0': { S: 'larry' },
		});

		expect(conditionExpression.names).toEqual({
			'#C_0': 'name',
		});
	});

	test('Between', () => {
		const conditionExpression = buildConditionExpression<User>([
			'age',
			'between',
			30,
			39,
		]);

		expect(conditionExpression.expression).toBe(
			'#C_0 BETWEEN :C_0_L AND :C_0_R',
		);
		expect(conditionExpression.values).toEqual({
			':C_0_L': { N: '30' },
			':C_0_R': { N: '39' },
		});

		expect(conditionExpression.names).toEqual({
			'#C_0': 'age',
		});
	});
});
