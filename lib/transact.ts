import type {
	AttributeValue,
	ConditionCheck,
	Delete,
	DynamoDB,
	Put,
	TransactGetItem,
	TransactWriteItem,
	TransactWriteItemsInput,
} from '@aws-sdk/client-dynamodb';
import { TransactionCanceledException } from '@aws-sdk/client-dynamodb';
import { condition, type ConditionExpression } from './expression/condition.js';
import type { WithoutReservedAttributes } from './facet.js';
import { applyCondition } from './condition.js';
import { PK, SK } from './keys.js';

/**
 * DynamoDB rejects `TransactWriteItems` and `TransactGetItems`
 * requests with more than 100 items.
 */
const MAX_TRANSACT_ITEMS = 100;

/**
 * The subset of a `Facet` that the transaction op builders require.
 * Structural, like `BatchWriteConnection`, so the builders can be
 * exercised without a full facet.
 */
export interface TransactFacet<T extends WithoutReservedAttributes<T>> {
	in(model: T): Record<string, AttributeValue>;
	out(record: Record<string, AttributeValue>): T;
	pk(model: Partial<T>): string;
	sk(model: Partial<T>): string;
	readonly connection: {
		dynamoDb: Pick<DynamoDB, 'transactWriteItems' | 'transactGetItems'>;
		tableName: string;
	};
}

/**
 * A single write operation in a {@link transactWrite} call. Produced
 * by a facet's `transaction.put`, `transaction.delete`, and
 * `transaction.check` builders.
 *
 * `T` is the type of the record the operation was built from. `Model`
 * is the facet's full record type, which is what DynamoDB returns as
 * the conflicting item when a condition fails.
 *
 * Builders marshall the record and compile the condition when they are
 * called. If the record contains a reserved attribute, the builder
 * throws immediately instead of reporting the error through the
 * `transactWrite` result.
 */
export interface TransactWriteOp<T = unknown, Model = T> {
	readonly kind: 'put' | 'delete' | 'check';
	readonly tableName: string;
	readonly dynamoDb: Pick<DynamoDB, 'transactWriteItems'>;
	/**
	 * The partition and sort key of the item this operation targets.
	 * A transaction may not include two operations that target the
	 * same item.
	 */
	readonly pk: string;
	readonly sk: string;
	readonly item: TransactWriteItem;
	/**
	 * The record the operation was built from: the full model for
	 * `put`, and the key fields for `delete` and `check`. This value
	 * is not sent to DynamoDB.
	 */
	readonly record: T;
	/**
	 * Converts a raw DynamoDB item into the facet's full record type.
	 * Used to parse the conflicting item on a canceled transaction.
	 */
	readonly out: (item: Record<string, AttributeValue>) => Model;
}

/**
 * A single read operation in a {@link transactGet} call. Produced by
 * a facet's `transaction.get` builder.
 */
export interface TransactGetOp<T = unknown> {
	readonly tableName: string;
	readonly dynamoDb: Pick<DynamoDB, 'transactGetItems'>;
	readonly item: TransactGetItem;
	/**
	 * Converts a raw DynamoDB item into the facet's model.
	 */
	readonly out: (item: Record<string, AttributeValue>) => T;
}

export interface TransactPutOptions<T> {
	condition?: ConditionExpression<T>;
}

export interface TransactDeleteOptions<T> {
	condition?: ConditionExpression<T>;
}

export interface TransactCheckOptions<T> {
	/**
	 * The condition to check. Required, because a check without a
	 * condition cannot fail and has no effect on the transaction.
	 */
	condition: ConditionExpression<T>;
}

/**
 * The operation builders returned by `Facet.transaction`.
 */
export interface FacetTransactionBuilders<
	T extends WithoutReservedAttributes<T>,
	PartitionKey extends keyof T,
	SortKey extends keyof T,
> {
	/**
	 * Writes the full record when the transaction commits. Replaces
	 * any existing item with the same key.
	 */
	put(record: T, options?: TransactPutOptions<T>): TransactWriteOp<T>;
	/**
	 * Deletes the record's item when the transaction commits.
	 */
	delete(
		record: Pick<T, PartitionKey | SortKey> & Partial<T>,
		options?: TransactDeleteOptions<T>,
	): TransactWriteOp<Pick<T, PartitionKey | SortKey> & Partial<T>, T>;
	/**
	 * Asserts a condition against the record's item without writing to
	 * it. If the condition fails, DynamoDB cancels the whole
	 * transaction.
	 */
	check(
		record: Pick<T, PartitionKey | SortKey> & Partial<T>,
		options: TransactCheckOptions<T>,
	): TransactWriteOp<Pick<T, PartitionKey | SortKey> & Partial<T>, T>;
	/**
	 * Reads the record's item in a {@link transactGet} call.
	 */
	get(query: Pick<T, PartitionKey | SortKey> & Partial<T>): TransactGetOp<T>;
}

export function buildTransactPutOp<T extends WithoutReservedAttributes<T>>(
	facet: TransactFacet<T>,
	record: T,
	options: TransactPutOptions<T> = {},
): TransactWriteOp<T> {
	const put: Put = {
		Item: facet.in(record),
		TableName: facet.connection.tableName,
		ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
	};
	if (options.condition) {
		applyCondition(put, options.condition);
	}
	return {
		kind: 'put',
		tableName: facet.connection.tableName,
		dynamoDb: facet.connection.dynamoDb,
		pk: facet.pk(record),
		sk: facet.sk(record),
		item: { Put: put },
		record,
		out: (item) => facet.out(item),
	};
}

export function buildTransactDeleteOp<
	T extends WithoutReservedAttributes<T>,
	U extends Partial<T>,
>(
	facet: TransactFacet<T>,
	record: U,
	options: TransactDeleteOptions<T> = {},
): TransactWriteOp<U, T> {
	const pk = facet.pk(record);
	const sk = facet.sk(record);
	const del: Delete = {
		Key: {
			[PK]: { S: pk },
			[SK]: { S: sk },
		},
		TableName: facet.connection.tableName,
		ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
	};
	if (options.condition) {
		applyCondition(del, options.condition);
	}
	return {
		kind: 'delete',
		tableName: facet.connection.tableName,
		dynamoDb: facet.connection.dynamoDb,
		pk,
		sk,
		item: { Delete: del },
		record,
		out: (item) => facet.out(item),
	};
}

export function buildTransactCheckOp<
	T extends WithoutReservedAttributes<T>,
	U extends Partial<T>,
>(
	facet: TransactFacet<T>,
	record: U,
	options: TransactCheckOptions<T>,
): TransactWriteOp<U, T> {
	const pk = facet.pk(record);
	const sk = facet.sk(record);
	/**
	 * `ConditionCheck.ConditionExpression` is a required field on the
	 * SDK type, so the check is built in one literal from the compiled
	 * condition rather than through `applyCondition`. Empty name and
	 * value maps are omitted because DynamoDB rejects them.
	 */
	const compiled = condition(options.condition);
	const check: ConditionCheck = {
		Key: {
			[PK]: { S: pk },
			[SK]: { S: sk },
		},
		TableName: facet.connection.tableName,
		ConditionExpression: compiled.expression,
		ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
	};
	/* v8 ignore next 3 */
	if (Object.keys(compiled.names).length > 0) {
		check.ExpressionAttributeNames = compiled.names;
	}
	if (Object.keys(compiled.values).length > 0) {
		check.ExpressionAttributeValues = compiled.values;
	}
	return {
		kind: 'check',
		tableName: facet.connection.tableName,
		dynamoDb: facet.connection.dynamoDb,
		pk,
		sk,
		item: { ConditionCheck: check },
		record,
		out: (item) => facet.out(item),
	};
}

export function buildTransactGetOp<T extends WithoutReservedAttributes<T>>(
	facet: TransactFacet<T>,
	query: Partial<T>,
): TransactGetOp<T> {
	return {
		tableName: facet.connection.tableName,
		dynamoDb: facet.connection.dynamoDb,
		item: {
			Get: {
				Key: {
					[PK]: { S: facet.pk(query) },
					[SK]: { S: facet.sk(query) },
				},
				TableName: facet.connection.tableName,
			},
		},
		out: (item) => facet.out(item),
	};
}

export interface TransactWriteOptions {
	/**
	 * Forwarded as `TransactWriteItemsInput.ClientRequestToken`.
	 * Retrying a transaction with the same token is idempotent.
	 */
	clientRequestToken?: string;
}

/**
 * The failure detail for one operation in a canceled transaction,
 * parsed from `TransactionCanceledException.CancellationReasons`.
 */
export interface TransactOpFailure<Model = unknown> {
	/**
	 * The position of the operation in the `ops` array.
	 */
	readonly index: number;
	/**
	 * The DynamoDB cancellation code, such as `ConditionalCheckFailed`.
	 */
	readonly code: string;
	readonly message?: string;
	/**
	 * The item as it currently exists in the table, parsed with the
	 * facet's validator.
	 *
	 * DynamoDB only returns the item when the failed condition's
	 * target item exists. A condition that fails because the item is
	 * missing, such as an `exists` guard, produces no item. Unset when
	 * DynamoDB returned no item or when the raw item did not parse;
	 * in the second case `conflictingItemRaw` still holds the data.
	 */
	readonly conflictingItem?: Model;
	/**
	 * The raw conflicting item, set whenever DynamoDB returned one.
	 */
	readonly conflictingItemRaw?: Record<string, AttributeValue>;
}

export interface TransactWriteResult<
	Ops extends readonly TransactWriteOp[] = readonly TransactWriteOp[],
> {
	wasSuccessful: boolean;
	error?: unknown;
	/**
	 * Set only when the failure is a `TransactionCanceledException`.
	 * One entry per operation, aligned with `ops`: `null` for
	 * operations that did not cause the cancellation, and a
	 * {@link TransactOpFailure} for each one that did.
	 */
	failures?: {
		[K in keyof Ops]: TransactOpFailure<
			Ops[K] extends TransactWriteOp<unknown, infer Model> ? Model : never
		> | null;
	};
}

/**
 * Commits up to 100 write operations atomically: either every
 * operation is applied, or none are. Operations can come from
 * different facets and target different tables, but every facet must
 * share the same DynamoDB client.
 *
 * The `ops` parameter preserves per-position types for array
 * literals, so `failures` can be destructured positionally with each
 * entry typed by its facet. A dynamically built array widens every
 * position to the union of the operations' types.
 *
 * This function does not throw. Failures resolve with
 * `wasSuccessful: false`; a canceled transaction also carries a
 * positional `failures` array.
 */
export async function transactWrite<Ops extends readonly TransactWriteOp[]>(
	ops: readonly [...Ops],
	options: TransactWriteOptions = {},
): Promise<TransactWriteResult<Ops>> {
	if (ops.length === 0) {
		return { wasSuccessful: true };
	}
	if (ops.length > MAX_TRANSACT_ITEMS) {
		return {
			wasSuccessful: false,
			error: new Error(
				`transactWrite accepts at most ${MAX_TRANSACT_ITEMS} operations; received ${ops.length}.`,
			),
		};
	}

	const dynamoDb = ops[0].dynamoDb;
	for (const op of ops) {
		if (op.dynamoDb !== dynamoDb) {
			return {
				wasSuccessful: false,
				error: new Error(
					'Every operation in a transactWrite must come from facets sharing the same DynamoDB client instance.',
				),
			};
		}
	}

	/**
	 * DynamoDB rejects a transaction that contains two operations
	 * targeting the same item. Failing before the network call lets
	 * the error name the offending positions instead of surfacing as
	 * an opaque ValidationException.
	 */
	const seenAt = new Map<string, number>();
	for (const [index, op] of ops.entries()) {
		/**
		 * `\x00` separates the segments so keys that would otherwise
		 * concatenate to the same string (a `pk` ending where another
		 * op's `sk` begins) cannot collide, matching the shard-hash
		 * separator in `keys.ts`.
		 */
		const target = `${op.tableName}\x00${op.pk}\x00${op.sk}`;
		const firstIndex = seenAt.get(target);
		if (firstIndex !== undefined) {
			return {
				wasSuccessful: false,
				error: new Error(
					`transactWrite operations ${firstIndex} and ${index} both target the same item (table "${op.tableName}", PK "${op.pk}", SK "${op.sk}"). DynamoDB transactions cannot operate on the same item twice.`,
				),
			};
		}
		seenAt.set(target, index);
	}

	const input: TransactWriteItemsInput = {
		TransactItems: ops.map((op) => op.item),
	};
	if (options.clientRequestToken) {
		input.ClientRequestToken = options.clientRequestToken;
	}

	try {
		await dynamoDb.transactWriteItems(input);
		return { wasSuccessful: true };
	} catch (error) {
		const failures = transactFailuresOf(
			error,
			ops,
		) as TransactWriteResult<Ops>['failures'];
		if (failures) {
			return { wasSuccessful: false, error, failures };
		}
		return { wasSuccessful: false, error };
	}
}

export interface TransactGetResult<
	Ops extends readonly TransactGetOp[] = readonly TransactGetOp[],
> {
	wasSuccessful: boolean;
	/**
	 * One entry per operation, in operation order. An entry is `null`
	 * when the item does not exist or the transaction failed.
	 */
	items: {
		[K in keyof Ops]:
			| (Ops[K] extends TransactGetOp<infer T> ? T : never)
			| null;
	};
	error?: unknown;
	/**
	 * Set only when the failure is a `TransactionCanceledException`.
	 * One entry per operation, aligned with the arguments: `null` for
	 * operations that did not cause the cancellation.
	 */
	failures?: {
		[K in keyof Ops]: TransactOpFailure<
			Ops[K] extends TransactGetOp<infer T> ? T : never
		> | null;
	};
}

/**
 * Reads up to 100 items atomically, as a consistent snapshot across
 * every operation. Operations can come from different facets and
 * target different tables, but every facet must share the same
 * DynamoDB client.
 *
 * Operations are rest arguments rather than an array so that each
 * position in `items` keeps its facet's record type. Spreading a
 * dynamically built array widens every position to the union of the
 * operations' types.
 *
 * This function does not throw. Failures resolve with
 * `wasSuccessful: false` and every entry in `items` set to `null`.
 */
export async function transactGet<Ops extends readonly TransactGetOp[]>(
	...ops: Ops
): Promise<TransactGetResult<Ops>> {
	type Items = TransactGetResult<Ops>['items'];
	const nullItems = ops.map(() => null) as Items;

	if (ops.length === 0) {
		return { wasSuccessful: true, items: nullItems };
	}
	if (ops.length > MAX_TRANSACT_ITEMS) {
		return {
			wasSuccessful: false,
			items: nullItems,
			error: new Error(
				`transactGet accepts at most ${MAX_TRANSACT_ITEMS} operations; received ${ops.length}.`,
			),
		};
	}

	const dynamoDb = ops[0].dynamoDb;
	for (const op of ops) {
		if (op.dynamoDb !== dynamoDb) {
			return {
				wasSuccessful: false,
				items: nullItems,
				error: new Error(
					'Every operation in a transactGet must come from facets sharing the same DynamoDB client instance.',
				),
			};
		}
	}

	try {
		const response = await dynamoDb.transactGetItems({
			TransactItems: ops.map((op) => op.item),
		});
		const responses = response.Responses ?? [];
		const items = ops.map((op, index) => {
			const raw = responses.at(index)?.Item;
			return raw ? op.out(raw) : null;
		});
		return { wasSuccessful: true, items: items as Items };
	} catch (error) {
		const failures = transactFailuresOf(
			error,
			ops,
		) as TransactGetResult<Ops>['failures'];
		if (failures) {
			return { wasSuccessful: false, items: nullItems, error, failures };
		}
		return { wasSuccessful: false, items: nullItems, error };
	}
}

/**
 * Parses a `TransactionCanceledException` into a positional failures
 * array. The SDK orders `CancellationReasons` to match the requested
 * items and uses the code `None` for items that did not cause the
 * cancellation, so those positions become `null`. When a reason
 * carries the conflicting item, the entry parses it with the
 * operation's `out` converter; a parse error leaves only the raw item.
 */
function transactFailuresOf(
	error: unknown,
	ops: readonly {
		readonly out: (item: Record<string, AttributeValue>) => unknown;
	}[],
): (TransactOpFailure | null)[] | undefined {
	if (
		!(error instanceof TransactionCanceledException) ||
		!error.CancellationReasons
	) {
		return undefined;
	}
	const reasons = error.CancellationReasons;
	return ops.map((op, index) => {
		const reason = reasons.at(index);
		if (reason?.Code === undefined || reason.Code === 'None') {
			return null;
		}
		const raw =
			reason.Item && Object.keys(reason.Item).length > 0
				? reason.Item
				: undefined;
		let conflictingItem: unknown;
		if (raw) {
			try {
				conflictingItem = op.out(raw);
			} catch {
				// The raw item stays available on conflictingItemRaw.
			}
		}
		return {
			index,
			code: reason.Code,
			message: reason.Message,
			conflictingItem,
			conflictingItemRaw: raw,
		};
	});
}
