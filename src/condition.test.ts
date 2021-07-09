import { buildConditionExpression } from './condition';

interface User {
	id: string;
	age: number;
	createdDate: Date;
	isActive?: boolean;
}

describe('condition.ts', () => {
	test('Condition Builder', () => {
		const conditionExpression = buildConditionExpression<User>([
			['age', '>=', 21],
			'OR',
			['age', '>=', 15],
		]);

		expect(conditionExpression.expression).toBe(
			'(#C_0_0_age >= :C_0_0_age) OR (#C_1_0_age >= :C_1_0_age)',
		);
		expect(conditionExpression.values).toEqual({
			':C_0_0_age': { N: '21' },
			':C_1_0_age': { N: '15' },
		});

		expect(conditionExpression.names).toEqual({
			'#C_0_0_age': 'age',
			'#C_1_0_age': 'age',
		});
	});
});
