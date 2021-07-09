import { Condition } from './condition-expression';

describe('Condition Expression Builder', () => {
	test('Build single condition', () => {
		const condition = Condition('name')
			.equals('joe')
			.and('age')
			.greaterThanOrEquals(27)
			.andNot(
				Condition('status').equals('active').or('status').equals('incomplete'),
			)
			.or(Condition('age').not().in([12, 19, 27]));

		const { attributeNames, attributeValues, statement } = condition.build([
			'con',
		]);

		expect(statement).toBe(
			'#con_0_name = :con_0_name AND #con_1_age >= :con_1_age AND NOT (#con_2_0_status = :con_2_0_status OR #con_2_1_status = :con_2_1_status) OR (NOT #con_3_0_age IN (:con_3_0_0_age, :con_3_0_1_age, :con_3_0_2_age))',
		);

		expect(attributeNames).toEqual({
			'#con_0_name': 'name',
			'#con_1_age': 'age',
			'#con_2_0_status': 'status',
			'#con_2_1_status': 'status',
			'#con_3_0_age': 'age',
		});

		expect(attributeValues).toEqual({
			':con_0_name': { S: 'joe' },
			':con_1_age': { N: '27' },
			':con_2_0_status': { S: 'active' },
			':con_2_1_status': { S: 'incomplete' },
			':con_3_0_0_age': { N: '12' },
			':con_3_0_1_age': { N: '19' },
			':con_3_0_2_age': { N: '27' },
		});
	});
});
