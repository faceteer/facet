import {
	DynamoDB,
	ResourceInUseException,
	TransactionCanceledException,
	type TransactGetItemsCommandInput,
	type TransactGetItemsCommandOutput,
	type TransactWriteItemsCommandInput,
	type TransactWriteItemsCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { condition } from '@faceteer/expression-builder';
import { vi } from 'vitest';
import { Facet } from './facet.js';
import { transactGet, transactWrite } from './transact.js';
import { wait } from './wait.js';

const ddb = new DynamoDB({
	region: 'us-east-1',
	endpoint: 'http://localhost:8000',
});

const tableName = 'TEST';

interface Account {
	accountId: string;
	status: string;
}

interface Order {
	accountId: string;
	orderId: string;
	note?: string;
}

const AccountFacet = new Facet({
	name: 'ACCOUNT',
	PK: {
		keys: ['accountId'],
		prefix: '#ACCOUNT',
	},
	SK: {
		keys: [],
		prefix: '#ACCOUNT',
	},
	validator: (input: unknown): Account => {
		return input as Account;
	},
	connection: {
		dynamoDb: ddb,
		tableName: tableName,
	},
});

const OrderFacet = new Facet({
	name: 'ORDER',
	PK: {
		keys: ['accountId'],
		prefix: '#ACCOUNT',
	},
	SK: {
		keys: ['orderId'],
		prefix: '#ORDER',
	},
	validator: (input: unknown): Order => {
		return input as Order;
	},
	connection: {
		dynamoDb: ddb,
		tableName: tableName,
	},
});

describe('transactions against DynamoDB Local', () => {
	beforeAll(async () => {
		await createTestTable();
	});

	test('transactWrite commits puts and deletes across facets atomically', async () => {
		const staleOrder: Order = { accountId: 'acct-1', orderId: 'order-old' };
		await OrderFacet.put(staleOrder);

		const account: Account = { accountId: 'acct-1', status: 'active' };
		const order: Order = {
			accountId: 'acct-1',
			orderId: 'order-1',
			note: 'first',
		};

		const result = await transactWrite([
			AccountFacet.transaction.put(account),
			OrderFacet.transaction.put(order),
			OrderFacet.transaction.delete(staleOrder),
		]);

		expect(result).toEqual({ wasSuccessful: true });
		expect(await AccountFacet.get({ accountId: 'acct-1' })).toEqual(account);
		expect(
			await OrderFacet.get({ accountId: 'acct-1', orderId: 'order-1' }),
		).toEqual(order);
		expect(
			await OrderFacet.get({ accountId: 'acct-1', orderId: 'order-old' }),
		).toBeNull();
	});

	test('a failed condition cancels every op in the transaction', async () => {
		await AccountFacet.put({ accountId: 'acct-2', status: 'suspended' });

		const order: Order = { accountId: 'acct-2', orderId: 'order-2' };
		const result = await transactWrite([
			OrderFacet.transaction.put(order),
			AccountFacet.transaction.put(
				{ accountId: 'acct-2', status: 'active' },
				{ condition: ['status', '=', 'active'] },
			),
		]);

		expect(result.wasSuccessful).toBe(false);
		expect(result.error).toBeInstanceOf(TransactionCanceledException);
		// `failures` is aligned with the ops array, typed per position.
		const [orderFailure, accountFailure] = result.failures ?? [];
		expect(orderFailure).toBeNull();
		expect(accountFailure).toMatchObject({
			index: 1,
			code: 'ConditionalCheckFailed',
		});
		// ALL_OLD returns the item as it currently exists in the table.
		expect(accountFailure?.conflictingItem).toEqual({
			accountId: 'acct-2',
			status: 'suspended',
		});
		expect(accountFailure?.conflictingItem?.status).toBe('suspended');
		expect(accountFailure?.conflictingItemRaw).toBeDefined();
		// The put at index 0 must not have been applied.
		expect(
			await OrderFacet.get({ accountId: 'acct-2', orderId: 'order-2' }),
		).toBeNull();
		expect(await AccountFacet.get({ accountId: 'acct-2' })).toEqual({
			accountId: 'acct-2',
			status: 'suspended',
		});
	});

	test('a condition that fails on a missing item carries no conflicting item', async () => {
		const result = await transactWrite([
			AccountFacet.transaction.check(
				{ accountId: 'acct-missing' },
				{ condition: ['status', '=', 'active'] },
			),
		]);

		expect(result.wasSuccessful).toBe(false);
		const [failure] = result.failures ?? [];
		expect(failure).toMatchObject({ index: 0, code: 'ConditionalCheckFailed' });
		expect(failure?.conflictingItem).toBeUndefined();
		expect(failure?.conflictingItemRaw).toBeUndefined();
	});

	test('a passing condition check guards a write without touching the checked item', async () => {
		await AccountFacet.put({ accountId: 'acct-3', status: 'active' });

		const order: Order = { accountId: 'acct-3', orderId: 'order-3' };
		const result = await transactWrite([
			OrderFacet.transaction.put(order),
			AccountFacet.transaction.check(
				{ accountId: 'acct-3' },
				{ condition: ['status', '=', 'active'] },
			),
		]);

		expect(result).toEqual({ wasSuccessful: true });
		expect(
			await OrderFacet.get({ accountId: 'acct-3', orderId: 'order-3' }),
		).toEqual(order);
		expect(await AccountFacet.get({ accountId: 'acct-3' })).toEqual({
			accountId: 'acct-3',
			status: 'active',
		});
	});

	test('transactGet reads across facets and keeps per-op types', async () => {
		const account: Account = { accountId: 'acct-4', status: 'active' };
		const order: Order = { accountId: 'acct-4', orderId: 'order-4' };
		await AccountFacet.put(account);
		await OrderFacet.put(order);

		const result = await transactGet(
			AccountFacet.transaction.get({ accountId: 'acct-4' }),
			OrderFacet.transaction.get({
				accountId: 'acct-4',
				orderId: 'order-4',
			}),
			OrderFacet.transaction.get({
				accountId: 'acct-4',
				orderId: 'order-missing',
			}),
		);

		expect(result.wasSuccessful).toBe(true);
		expect(result.items).toEqual([account, order, null]);
		// Each position keeps its own facet's type.
		expect(result.items[0]?.status).toBe('active');
		expect(result.items[1]?.orderId).toBe('order-4');
	});

	test('two ops targeting the same item are rejected before any network call', async () => {
		const account: Account = { accountId: 'acct-5', status: 'active' };
		const result = await transactWrite([
			AccountFacet.transaction.put(account),
			AccountFacet.transaction.put({ ...account, status: 'suspended' }),
		]);

		expect(result.wasSuccessful).toBe(false);
		expect(result.error).toBeInstanceOf(Error);
		expect((result.error as Error).message).toContain('operations 0 and 1');
		expect(await AccountFacet.get({ accountId: 'acct-5' })).toBeNull();
	});

	test('more than 100 write ops are rejected before any network call', async () => {
		const ops = Array.from({ length: 101 }, (_, index) =>
			AccountFacet.transaction.delete({ accountId: `bulk-${index}` }),
		);

		const result = await transactWrite(ops);

		expect(result.wasSuccessful).toBe(false);
		expect((result.error as Error).message).toContain(
			'at most 100 operations; received 101',
		);
	});
});

interface Item {
	pk: string;
	sk: string;
	value?: string;
}

function buildMockedFacet() {
	const mockedDdb = new DynamoDB({
		region: 'us-east-1',
		endpoint: 'http://localhost:8000',
	});
	const facet = new Facet<Item, 'pk', 'sk'>({
		name: 'Item',
		PK: { keys: ['pk'], prefix: 'PK' },
		SK: { keys: ['sk'], prefix: 'SK' },
		validator: (input) => input as Item,
		connection: {
			dynamoDb: mockedDdb,
			tableName: tableName,
		},
	});
	return { facet, mockedDdb };
}

function spyOnTransactWrite(
	mockedDdb: DynamoDB,
	implementation: (
		input: TransactWriteItemsCommandInput,
	) => Promise<TransactWriteItemsCommandOutput>,
) {
	const calls: TransactWriteItemsCommandInput[] = [];
	const spy = vi
		.spyOn(mockedDdb, 'transactWriteItems')
		.mockImplementation(async (input: TransactWriteItemsCommandInput) => {
			calls.push(input);
			return implementation(input);
		});
	return { spy, calls };
}

function spyOnTransactGet(
	mockedDdb: DynamoDB,
	implementation: (
		input: TransactGetItemsCommandInput,
	) => Promise<TransactGetItemsCommandOutput>,
) {
	const calls: TransactGetItemsCommandInput[] = [];
	const spy = vi
		.spyOn(mockedDdb, 'transactGetItems')
		.mockImplementation(async (input: TransactGetItemsCommandInput) => {
			calls.push(input);
			return implementation(input);
		});
	return { spy, calls };
}

describe('transactWrite with a mocked client', () => {
	test('ops from different client instances are rejected without a call', async () => {
		const first = buildMockedFacet();
		const second = buildMockedFacet();
		const firstSpy = spyOnTransactWrite(first.mockedDdb, () =>
			Promise.resolve({ $metadata: {} }),
		);
		const secondSpy = spyOnTransactWrite(second.mockedDdb, () =>
			Promise.resolve({ $metadata: {} }),
		);

		const result = await transactWrite([
			first.facet.transaction.put({ pk: 'a', sk: 'a' }),
			second.facet.transaction.put({ pk: 'b', sk: 'b' }),
		]);

		expect(result.wasSuccessful).toBe(false);
		expect((result.error as Error).message).toContain(
			'same DynamoDB client instance',
		);
		expect(firstSpy.spy).not.toHaveBeenCalled();
		expect(secondSpy.spy).not.toHaveBeenCalled();
	});

	test('clientRequestToken is forwarded, and omitted when not provided', async () => {
		const { facet, mockedDdb } = buildMockedFacet();
		const { calls } = spyOnTransactWrite(mockedDdb, () =>
			Promise.resolve({ $metadata: {} }),
		);

		await transactWrite([facet.transaction.put({ pk: 'a', sk: 'a' })], {
			clientRequestToken: 'token-1',
		});
		await transactWrite([facet.transaction.put({ pk: 'a', sk: 'a' })]);

		expect(calls[0].ClientRequestToken).toBe('token-1');
		expect(calls[1]).not.toHaveProperty('ClientRequestToken');
	});

	test('a non-cancellation error carries no failures', async () => {
		const { facet, mockedDdb } = buildMockedFacet();
		const failure = new Error('socket hang up');
		spyOnTransactWrite(mockedDdb, () => Promise.reject(failure));

		const result = await transactWrite([
			facet.transaction.put({ pk: 'a', sk: 'a' }),
		]);

		expect(result.wasSuccessful).toBe(false);
		expect(result.error).toBe(failure);
		expect(result.failures).toBeUndefined();
	});

	test('a cancellation without reasons carries no failures', async () => {
		const { facet, mockedDdb } = buildMockedFacet();
		const failure = new TransactionCanceledException({
			message: 'Transaction cancelled',
			$metadata: {},
		});
		spyOnTransactWrite(mockedDdb, () => Promise.reject(failure));

		const result = await transactWrite([
			facet.transaction.put({ pk: 'a', sk: 'a' }),
		]);

		expect(result.wasSuccessful).toBe(false);
		expect(result.error).toBe(failure);
		expect(result.failures).toBeUndefined();
	});

	test('failures keep op positions and stay null for None entries', async () => {
		const { facet, mockedDdb } = buildMockedFacet();
		spyOnTransactWrite(mockedDdb, () =>
			Promise.reject(
				new TransactionCanceledException({
					message: 'Transaction cancelled',
					$metadata: {},
					CancellationReasons: [
						{ Code: 'None' },
						{ Code: 'ConditionalCheckFailed', Message: 'The condition failed' },
						{ Code: 'None' },
						{ Code: 'TransactionConflict' },
					],
				}),
			),
		);

		const result = await transactWrite([
			facet.transaction.put({ pk: 'a', sk: 'a' }),
			facet.transaction.put({ pk: 'b', sk: 'b' }),
			facet.transaction.put({ pk: 'c', sk: 'c' }),
			facet.transaction.put({ pk: 'd', sk: 'd' }),
		]);

		expect(result.failures).toEqual([
			null,
			{
				index: 1,
				code: 'ConditionalCheckFailed',
				message: 'The condition failed',
			},
			null,
			{ index: 3, code: 'TransactionConflict' },
		]);
	});

	test('missing or code-less reasons become null failure entries', async () => {
		const { facet, mockedDdb } = buildMockedFacet();
		spyOnTransactWrite(mockedDdb, () =>
			Promise.reject(
				new TransactionCanceledException({
					message: 'Transaction cancelled',
					$metadata: {},
					CancellationReasons: [{ Code: 'ConditionalCheckFailed' }, {}],
				}),
			),
		);

		const result = await transactWrite([
			facet.transaction.put({ pk: 'a', sk: 'a' }),
			facet.transaction.put({ pk: 'b', sk: 'b' }),
			facet.transaction.put({ pk: 'c', sk: 'c' }),
		]);

		expect(result.failures).toEqual([
			{ index: 0, code: 'ConditionalCheckFailed' },
			// A reason with no code, and a position with no reason at all.
			null,
			null,
		]);
	});

	test('a returned conflicting item is parsed with the facet converter', async () => {
		const { facet, mockedDdb } = buildMockedFacet();
		spyOnTransactWrite(mockedDdb, () =>
			Promise.reject(
				new TransactionCanceledException({
					message: 'Transaction cancelled',
					$metadata: {},
					CancellationReasons: [
						{
							Code: 'ConditionalCheckFailed',
							Item: {
								PK: { S: 'PK_a' },
								SK: { S: 'SK_a' },
								facet: { S: 'Item' },
								pk: { S: 'a' },
								sk: { S: 'a' },
								value: { S: 'current' },
							},
						},
					],
				}),
			),
		);

		const result = await transactWrite([
			facet.transaction.put({ pk: 'a', sk: 'a', value: 'attempted' }),
		]);

		const [failure] = result.failures ?? [];
		expect(failure?.conflictingItem).toEqual({
			pk: 'a',
			sk: 'a',
			value: 'current',
		});
		expect(failure?.conflictingItemRaw).toBeDefined();
	});

	test('an empty conflicting item map is treated as absent', async () => {
		const { facet, mockedDdb } = buildMockedFacet();
		spyOnTransactWrite(mockedDdb, () =>
			Promise.reject(
				new TransactionCanceledException({
					message: 'Transaction cancelled',
					$metadata: {},
					CancellationReasons: [{ Code: 'ConditionalCheckFailed', Item: {} }],
				}),
			),
		);

		const result = await transactWrite([
			facet.transaction.put({ pk: 'a', sk: 'a' }),
		]);

		const [failure] = result.failures ?? [];
		expect(failure?.conflictingItem).toBeUndefined();
		expect(failure?.conflictingItemRaw).toBeUndefined();
	});

	test('a conflicting item that fails validation keeps only the raw item', async () => {
		const mockedDdb = new DynamoDB({
			region: 'us-east-1',
			endpoint: 'http://localhost:8000',
		});
		const strictFacet = new Facet<Item, 'pk', 'sk'>({
			name: 'Item',
			PK: { keys: ['pk'], prefix: 'PK' },
			SK: { keys: ['sk'], prefix: 'SK' },
			validator: () => {
				throw new Error('invalid record');
			},
			connection: { dynamoDb: mockedDdb, tableName },
		});
		const rawItem = {
			PK: { S: 'PK_a' },
			SK: { S: 'SK_a' },
			pk: { S: 'a' },
			sk: { S: 'a' },
		};
		spyOnTransactWrite(mockedDdb, () =>
			Promise.reject(
				new TransactionCanceledException({
					message: 'Transaction cancelled',
					$metadata: {},
					CancellationReasons: [
						{ Code: 'ConditionalCheckFailed', Item: rawItem },
					],
				}),
			),
		);

		const result = await transactWrite([
			strictFacet.transaction.delete({ pk: 'a', sk: 'a' }),
		]);

		const [failure] = result.failures ?? [];
		expect(failure?.conflictingItem).toBeUndefined();
		expect(failure?.conflictingItemRaw).toEqual(rawItem);
	});

	test('an empty op list resolves without a network call', async () => {
		const { mockedDdb } = buildMockedFacet();
		const { spy } = spyOnTransactWrite(mockedDdb, () =>
			Promise.resolve({ $metadata: {} }),
		);

		const result = await transactWrite([]);

		expect(result).toEqual({ wasSuccessful: true });
		expect(spy).not.toHaveBeenCalled();
	});
});

describe('transactGet with a mocked client', () => {
	test('ops from different client instances are rejected without a call', async () => {
		const first = buildMockedFacet();
		const second = buildMockedFacet();
		const firstSpy = spyOnTransactGet(first.mockedDdb, () =>
			Promise.resolve({ $metadata: {} }),
		);
		const secondSpy = spyOnTransactGet(second.mockedDdb, () =>
			Promise.resolve({ $metadata: {} }),
		);

		const result = await transactGet(
			first.facet.transaction.get({ pk: 'a', sk: 'a' }),
			second.facet.transaction.get({ pk: 'b', sk: 'b' }),
		);

		expect(result.wasSuccessful).toBe(false);
		expect((result.error as Error).message).toContain(
			'same DynamoDB client instance',
		);
		expect(result.items).toEqual([null, null]);
		expect(firstSpy.spy).not.toHaveBeenCalled();
		expect(secondSpy.spy).not.toHaveBeenCalled();
	});

	test('more than 100 get ops are rejected without a call', async () => {
		const { facet, mockedDdb } = buildMockedFacet();
		const { spy } = spyOnTransactGet(mockedDdb, () =>
			Promise.resolve({ $metadata: {} }),
		);
		const ops = Array.from({ length: 101 }, (_, index) =>
			facet.transaction.get({ pk: `pk-${index}`, sk: 'sk' }),
		);

		const result = await transactGet(...ops);

		expect(result.wasSuccessful).toBe(false);
		expect((result.error as Error).message).toContain(
			'at most 100 operations; received 101',
		);
		expect(result.items).toHaveLength(101);
		expect(spy).not.toHaveBeenCalled();
	});

	test('a missing Responses array yields all-null items', async () => {
		const { facet, mockedDdb } = buildMockedFacet();
		spyOnTransactGet(mockedDdb, () => Promise.resolve({ $metadata: {} }));

		const result = await transactGet(
			facet.transaction.get({ pk: 'a', sk: 'a' }),
			facet.transaction.get({ pk: 'b', sk: 'b' }),
		);

		expect(result.wasSuccessful).toBe(true);
		expect(result.items).toEqual([null, null]);
	});

	test('a canceled read resolves with reasons and all-null items', async () => {
		const { facet, mockedDdb } = buildMockedFacet();
		spyOnTransactGet(mockedDdb, () =>
			Promise.reject(
				new TransactionCanceledException({
					message: 'Transaction cancelled',
					$metadata: {},
					CancellationReasons: [
						{ Code: 'TransactionConflict', Message: 'Conflict' },
						{ Code: 'None' },
					],
				}),
			),
		);

		const result = await transactGet(
			facet.transaction.get({ pk: 'a', sk: 'a' }),
			facet.transaction.get({ pk: 'b', sk: 'b' }),
		);

		expect(result.wasSuccessful).toBe(false);
		expect(result.error).toBeInstanceOf(TransactionCanceledException);
		expect(result.items).toEqual([null, null]);
		expect(result.failures).toEqual([
			{ index: 0, code: 'TransactionConflict', message: 'Conflict' },
			null,
		]);
	});

	test('a non-cancellation error carries no failures', async () => {
		const { facet, mockedDdb } = buildMockedFacet();
		const failure = new Error('socket hang up');
		spyOnTransactGet(mockedDdb, () => Promise.reject(failure));

		const result = await transactGet(
			facet.transaction.get({ pk: 'a', sk: 'a' }),
		);

		expect(result.wasSuccessful).toBe(false);
		expect(result.error).toBe(failure);
		expect(result.failures).toBeUndefined();
		expect(result.items).toEqual([null]);
	});

	test('zero ops resolve without a network call', async () => {
		const { mockedDdb } = buildMockedFacet();
		const { spy } = spyOnTransactGet(mockedDdb, () =>
			Promise.resolve({ $metadata: {} }),
		);

		const result = await transactGet();

		expect(result).toEqual({ wasSuccessful: true, items: [] });
		expect(spy).not.toHaveBeenCalled();
	});
});

describe('transaction op builders', () => {
	test('put throws synchronously on a reserved attribute', () => {
		const record = { pk: 'a', sk: 'a', PK: 'collision' } as unknown as Item;
		const { facet } = buildMockedFacet();

		expect(() => facet.transaction.put(record)).toThrow(
			/reserved attribute "PK"/,
		);
	});

	test('put and delete ops carry the facet key and table', () => {
		const { facet } = buildMockedFacet();

		const putOp = facet.transaction.put({ pk: 'a', sk: 'b', value: 'v' });
		expect(putOp.kind).toBe('put');
		expect(putOp.tableName).toBe(tableName);
		expect(putOp.pk).toBe('PK_a');
		expect(putOp.sk).toBe('SK_b');
		expect(putOp.item.Put?.Item?.value).toEqual({ S: 'v' });
		expect(putOp.item.Put?.ReturnValuesOnConditionCheckFailure).toBe('ALL_OLD');
		// `out` converts a raw item back into the model.
		expect(putOp.out(putOp.item.Put?.Item ?? {})).toEqual({
			pk: 'a',
			sk: 'b',
			value: 'v',
		});

		const deleteOp = facet.transaction.delete({ pk: 'a', sk: 'b' });
		expect(deleteOp.kind).toBe('delete');
		expect(deleteOp.item.Delete?.Key).toEqual({
			PK: { S: 'PK_a' },
			SK: { S: 'SK_b' },
		});
		expect(deleteOp.item.Delete?.ReturnValuesOnConditionCheckFailure).toBe(
			'ALL_OLD',
		);
	});

	test('delete only carries a condition when one is given', () => {
		const { facet } = buildMockedFacet();

		const bare = facet.transaction.delete({ pk: 'a', sk: 'b' });
		expect(bare.item.Delete).not.toHaveProperty('ConditionExpression');

		const conditional = facet.transaction.delete(
			{ pk: 'a', sk: 'b' },
			{ condition: ['value', '=', 'v'] },
		);
		expect(conditional.item.Delete?.ConditionExpression).toBeDefined();
		expect(conditional.item.Delete?.ExpressionAttributeValues).toBeDefined();
	});

	test('check compiles its condition like the expression builder does', () => {
		const { facet } = buildMockedFacet();
		const compiled = condition<Item>(['value', '=', 'v']);

		const op = facet.transaction.check(
			{ pk: 'a', sk: 'b' },
			{ condition: ['value', '=', 'v'] },
		);

		expect(op.kind).toBe('check');
		expect(op.item.ConditionCheck?.ConditionExpression).toBe(
			compiled.expression,
		);
		expect(op.item.ConditionCheck?.ExpressionAttributeNames).toEqual(
			compiled.names,
		);
		expect(op.item.ConditionCheck?.ExpressionAttributeValues).toEqual(
			compiled.values,
		);
		expect(op.item.ConditionCheck?.ReturnValuesOnConditionCheckFailure).toBe(
			'ALL_OLD',
		);
		// `out` converts a raw conflicting item back into the model.
		expect(
			op.out({
				PK: { S: 'PK_a' },
				SK: { S: 'SK_b' },
				facet: { S: 'Item' },
				pk: { S: 'a' },
				sk: { S: 'b' },
			}),
		).toEqual({ pk: 'a', sk: 'b' });
	});

	test('check omits the value map when the condition produces no values', () => {
		const { facet } = buildMockedFacet();

		const op = facet.transaction.check(
			{ pk: 'a', sk: 'b' },
			{ condition: ['value', 'exists'] },
		);

		expect(op.item.ConditionCheck?.ConditionExpression).toBeDefined();
		expect(op.item.ConditionCheck).not.toHaveProperty(
			'ExpressionAttributeValues',
		);
	});
});

/**
 * Create the Dynamo DB table for testing. The transaction facets only
 * use the base `PK`/`SK` key schema, so no GSIs are declared here.
 */
async function createTestTable(): Promise<void> {
	let active = false;

	try {
		await ddb.createTable({
			TableName: tableName,
			AttributeDefinitions: [
				{ AttributeName: 'PK', AttributeType: 'S' },
				{ AttributeName: 'SK', AttributeType: 'S' },
			],
			KeySchema: [
				{ AttributeName: 'PK', KeyType: 'HASH' },
				{ AttributeName: 'SK', KeyType: 'RANGE' },
			],
			BillingMode: 'PAY_PER_REQUEST',
		});
	} catch (error) {
		if (!(error instanceof ResourceInUseException)) {
			throw error;
		}
		await ddb.deleteTable({
			TableName: tableName,
		});

		await createTestTable();
	}

	/**
	 * Wait for the table to be ready
	 */
	while (!active) {
		const status = await ddb.describeTable({
			TableName: tableName,
		});

		await wait(100);
		if (status.Table?.TableStatus === 'ACTIVE') {
			active = true;
		}
	}
}
