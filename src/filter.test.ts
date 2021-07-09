import { Filter } from './filter';

type User = {
	id: string;
	name: string;
	email: string;
	age: number;
	createdAt: Date;
};

describe('Filter Test', () => {
	test('Builds correct string', () => {
		const filter: Filter<User> = ['name', 'begins_with', 'alex'];
	});
});
