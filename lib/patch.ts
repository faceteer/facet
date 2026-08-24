import {
	ConditionalCheckFailedException,
	type AttributeValue,
	type DynamoDB,
	type GetItemInput,
	type UpdateItemInput,
} from '@aws-sdk/client-dynamodb';
import type { AttributeMap } from './converter/converter.js';
import { condition, type ConditionExpression } from './expression/condition.js';
import type { FacetIndex, WithoutReservedAttributes } from './facet.js';
import type { Keys } from './keys.js';
import { normalizeTtl } from './ttl.js';

/**
 * The fields of `T` that a patch may change: everything except the
 * fields that compose the base partition and sort keys, all optional.
 *
 * Computed per union member of `T` (the `T extends T` distribution
 * idiom, same as `Keys<T>` in keys.ts) so a facet over a discriminated
 * union keeps every variant's own fields patchable. Known limit: an
 * object literal that omits the discriminant can mix fields from two
 * variants, because a property is excess only when no union member
 * declares it. Include the discriminant in the patch to get full
 * per-variant checking.
 */
export type PatchOf<
	T extends WithoutReservedAttributes<T>,
	PK extends Keys<T>,
	SK extends Keys<T>,
> = T extends T ? Partial<Omit<T, PK | SK>> : never;

export interface PatchOptions<T> {
	/**
	 * A condition that must hold on the stored record for the patch to
	 * apply. Same tuple grammar as `PutOptions` and `DeleteOptions`.
	 * A failed condition resolves as `wasSuccessful: false`.
	 */
	condition?: ConditionExpression<T>;
	/**
	 * What to do when an affected composite key can't be recomputed
	 * from `query` and `patch` alone.
	 *
	 * - `'read'` (default): read the record and recompute every
	 *   affected key from its current values. Two round trips.
	 * - `'strict'`: never read. An incomplete patch fails to compile
	 *   at the call site, and a {@link PatchMissingKeyInputsError} is
	 *   reported if one gets through anyway (for example through a
	 *   type-erased facet).
	 */
	missingKeyInputs?: 'read' | 'strict';
}

/**
 * One `{ inputs: ... }` wrapper per synthetic key registered on the
 * facet type `This`, holding the union of model fields (key fields
 * plus shard keys) that compose that key. Extracted from the index
 * accessors that `addIndex` merges into the facet's type.
 */
/* eslint-disable @typescript-eslint/no-unused-vars -- the unused infer slots position the key and shard parameters */
export type PatchKeyInputGroups<This> = NonNullable<
	{
		[K in keyof This]: This[K] extends FacetIndex<
			infer _T,
			infer _PK,
			infer _SK,
			infer GSIPK,
			infer GSISK,
			infer _PV,
			infer GSIPKS,
			infer GSISKS
		>
			? { inputs: GSIPK | GSIPKS } | { inputs: GSISK | GSISKS }
			: never;
	}[keyof This]
	// NonNullable strips the `undefined` that optional facet
	// properties (like `ttl`) fold into the indexed union.
>;
/* eslint-enable @typescript-eslint/no-unused-vars */

/**
 * The fields a strict patch still needs: for every synthetic key the
 * patch touches, the inputs found in neither the patch, the query,
 * nor the base key fields. `never` when the patch is self-sufficient.
 */
export type MissingPatchKeyInputs<
	Groups,
	P,
	Q,
	PK extends PropertyKey,
	SK extends PropertyKey,
> = Groups extends { inputs: infer U extends PropertyKey }
	? [Extract<keyof P, U>] extends [never]
		? never
		: Exclude<U, keyof P | keyof Q | PK | SK>
	: never;

/**
 * Brands the strict-mode demand property so no options object can
 * satisfy it. The symbol is never exported and has no runtime value.
 */
declare const patchDemandBrand: unique symbol;

/**
 * The options shape of a strict-mode patch. When the patch leaves a
 * touched key's inputs unresolved, the required
 * `'Supply these key inputs in query or patch'` property makes the
 * call fail to compile, naming the missing fields in the error. The
 * property is branded with an unexported symbol, so it can't be
 * supplied to bypass the check.
 */
export type StrictPatchOptions<D extends PropertyKey> = [D] extends [never]
	? { missingKeyInputs: 'strict' }
	: {
			missingKeyInputs: 'strict';
			'Supply these key inputs in query or patch': readonly D[] & {
				readonly [patchDemandBrand]: typeof patchDemandBrand;
			};
		};

/**
 * The successful result of a `Facet.patch` call.
 */
export interface PatchSuccess<T> {
	wasSuccessful: true;
	/**
	 * The full post-patch record, read back through `ReturnValues:
	 * 'ALL_NEW'` and validated by the facet's `validator`.
	 */
	record: T;
	/**
	 * True when the patch issued a fallback read to resolve missing
	 * key inputs, adding a second round trip. `Facet.patchInputs`
	 * reports ahead of time which fields to supply to avoid the read.
	 */
	usedFallbackRead: boolean;
}

/**
 * The failed result of a `Facet.patch` call.
 */
export interface PatchFailure<T> {
	wasSuccessful: false;
	error: unknown;
	/**
	 * The stored record that failed a condition check, when DynamoDB
	 * returned one and it parsed with the facet's `validator`. Unset
	 * for non-conditional failures, when the condition failed because
	 * the record doesn't exist, or when the returned item didn't
	 * validate. In the last case, {@link conflictingItemRaw} still
	 * holds the data.
	 */
	conflictingItem?: T;
	/**
	 * The raw conflicting item, set whenever DynamoDB returned one.
	 */
	conflictingItemRaw?: AttributeMap;
	/**
	 * True when the patch issued a fallback read before failing.
	 */
	usedFallbackRead: boolean;
}

export type PatchSingleItemResponse<T> = PatchSuccess<T> | PatchFailure<T>;

/**
 * Reported when `missingKeyInputs: 'strict'` is set and an affected
 * composite key can't be recomputed from `query` and `patch` alone.
 * Strict mode normally rejects such a patch at compile time; this is
 * the runtime backstop for calls the compiler can't see, such as a
 * facet widened to a type without its index accessors.
 */
export class PatchMissingKeyInputsError extends Error {
	/**
	 * The model fields that are needed but were not supplied.
	 */
	readonly fields: readonly string[];
	/**
	 * The synthetic key attributes that depend on those fields.
	 */
	readonly attributeNames: readonly string[];

	constructor(fields: readonly string[], attributeNames: readonly string[]) {
		super(
			`Cannot recompute ${attributeNames.join(', ')} without the field(s) ${fields.join(
				', ',
			)}. Supply them in the query or patch, or use missingKeyInputs: 'read'.`,
		);
		this.name = 'PatchMissingKeyInputsError';
		this.fields = fields;
		this.attributeNames = attributeNames;
	}
}

/**
 * Reported when a patch touches a field that composes the base
 * partition or sort key (including base shard keys). Changing a
 * record's identity requires a delete and a put.
 */
export class PatchIdentityFieldError extends Error {
	/**
	 * The identity fields the patch attempted to change.
	 */
	readonly fields: readonly string[];

	constructor(fields: readonly string[]) {
		super(
			`Cannot patch field(s) ${fields.join(', ')}: they compose this facet's PK or SK. Changing a record's identity requires a delete and a put.`,
		);
		this.name = 'PatchIdentityFieldError';
		this.fields = fields;
	}
}

/**
 * Reported when the `missingKeyInputs: 'read'` fallback found no
 * record at the patch's PK and SK.
 */
export class PatchItemNotFoundError extends Error {
	constructor() {
		super('No record exists at this PK and SK, and patch never upserts.');
		this.name = 'PatchItemNotFoundError';
	}
}

/**
 * Reported when the patch object has no fields.
 */
export class EmptyPatchError extends Error {
	constructor() {
		super('patch() requires at least one field to update.');
		this.name = 'EmptyPatchError';
	}
}

/**
 * The slice of a facet that `patchSingleItem` needs. Internal: built
 * by `Facet.patch` from the facet's own (partly private) state.
 *
 * `marshalValue` must marshal exactly as `Facet.in` would (honoring
 * the facet's `dateFormat` and `convertEmptyValues`), so patched
 * values and guard comparisons match what put wrote.
 */
export interface PatchFacet<T extends WithoutReservedAttributes<T>> {
	pk(model: Partial<T>): string;
	sk(model: Partial<T>): string;
	out(record: AttributeMap): T;
	marshalValue(value: unknown): AttributeValue | undefined;
	readonly ttl?: Keys<T>;
	readonly connection: {
		dynamoDb: DynamoDB;
		tableName: string;
	};
}

/**
 * One synthetic key attribute a patch may need to recompute: its
 * attribute name, the model fields that feed it (key fields plus
 * shard keys), and its key builder. Internal.
 */
export interface PatchKeyTarget<T> {
	attributeName: string;
	inputs: readonly PropertyKey[];
	build: (model: Partial<T>) => string;
}

/**
 * A value a key recompute used for one input field, plus where it
 * came from. The source decides whether the write carries a guard.
 */
interface ResolvedInput {
	value: unknown;
	source: 'patch' | 'identity' | 'hint' | 'read';
}

/**
 * `true` for values that can contribute to a composite key.
 * `buildKey` renders primitives and Dates and silently omits
 * everything else, so guards are only worth writing for these.
 */
function contributesToKeys(value: unknown): boolean {
	switch (typeof value) {
		case 'bigint':
		case 'boolean':
		case 'number':
		case 'string':
			return true;
		default:
			return value instanceof Date;
	}
}

export async function patchSingleItem<T extends WithoutReservedAttributes<T>>(
	facet: PatchFacet<T>,
	gsiTargets: readonly PatchKeyTarget<T>[],
	identityFields: ReadonlySet<PropertyKey>,
	query: Partial<T>,
	patch: Partial<T>,
	options: PatchOptions<T> = {},
): Promise<PatchSingleItemResponse<T>> {
	let usedFallbackRead = false;
	try {
		const queryRecord = query as Record<PropertyKey, unknown>;
		const patchRecord = patch as Record<PropertyKey, unknown>;
		const touched = new Set<PropertyKey>(Object.keys(patchRecord));

		const key: AttributeMap = {
			PK: { S: facet.pk(query) },
			SK: { S: facet.sk(query) },
		};

		/**
		 * Work out which synthetic keys the patch invalidates and which
		 * fields each recompute needs.
		 */
		const affected = gsiTargets.filter((target) =>
			target.inputs.some((field) => touched.has(field)),
		);
		const inputFields = new Set<PropertyKey>();
		for (const target of affected) {
			for (const field of target.inputs) {
				inputFields.add(field);
			}
		}

		const missing: string[] = [];
		for (const field of inputFields) {
			if (
				!touched.has(field) &&
				!identityFields.has(field) &&
				!Object.prototype.hasOwnProperty.call(query, field)
			) {
				missing.push(String(field));
			}
		}

		/**
		 * Resolve a value for every input field. Values from the patch
		 * or from base identity fields need no guard; values from the
		 * caller's extra query fields (hints) or from the fallback read
		 * are unverified, so the write asserts them below.
		 */
		const resolved = new Map<PropertyKey, ResolvedInput>();
		let readItem: AttributeMap | undefined;

		if (missing.length > 0) {
			if (options.missingKeyInputs === 'strict') {
				const missingSet = new Set(missing);
				const dependents = affected
					.filter((target) =>
						target.inputs.some((field) => missingSet.has(String(field))),
					)
					.map((target) => target.attributeName);
				return {
					wasSuccessful: false,
					error: new PatchMissingKeyInputsError(missing, dependents),
					usedFallbackRead,
				};
			}

			const getInput: GetItemInput = {
				TableName: facet.connection.tableName,
				Key: key,
			};
			usedFallbackRead = true;
			const getResult = await facet.connection.dynamoDb.getItem(getInput);
			if (!getResult.Item) {
				return {
					wasSuccessful: false,
					error: new PatchItemNotFoundError(),
					usedFallbackRead,
				};
			}
			readItem = getResult.Item;
			/**
			 * The validated record restores real pre-marshall types
			 * (a stored date string becomes a `Date` again), which the
			 * key builders need. Raw attribute values can't feed a
			 * recompute: they carry the marshalled representation.
			 */
			const readRecord = facet.out(readItem) as Record<PropertyKey, unknown>;
			for (const field of inputFields) {
				if (touched.has(field)) {
					resolved.set(field, { value: patchRecord[field], source: 'patch' });
				} else if (identityFields.has(field)) {
					resolved.set(field, {
						value: queryRecord[field],
						source: 'identity',
					});
				} else {
					/**
					 * Read values replace caller hints: the read is
					 * fresher, and its guards subsume the hint guards.
					 */
					resolved.set(field, { value: readRecord[field], source: 'read' });
				}
			}
		} else {
			for (const field of inputFields) {
				if (touched.has(field)) {
					resolved.set(field, { value: patchRecord[field], source: 'patch' });
				} else if (identityFields.has(field)) {
					resolved.set(field, {
						value: queryRecord[field],
						source: 'identity',
					});
				} else {
					resolved.set(field, { value: queryRecord[field], source: 'hint' });
				}
			}
		}

		const effectiveModel: Record<PropertyKey, unknown> = {};
		for (const [field, input] of resolved) {
			effectiveModel[field] = input.value;
		}

		/**
		 * Build the update expression. DynamoDB allows each action
		 * keyword at most once, so clauses group under a single SET and
		 * a single REMOVE.
		 */
		const names: Record<string, string> = {};
		const values: Record<string, AttributeValue> = {};
		const setClauses: string[] = [];
		const removeClauses: string[] = [];
		let updatePlaceholder = 0;

		const addSet = (attributeName: string, value: AttributeValue): void => {
			const id = updatePlaceholder++;
			names[`#U_${id}`] = attributeName;
			values[`:U_${id}`] = value;
			setClauses.push(`#U_${id} = :U_${id}`);
		};
		const addRemove = (attributeName: string): void => {
			const id = updatePlaceholder++;
			names[`#U_${id}`] = attributeName;
			removeClauses.push(`#U_${id}`);
		};

		for (const field of touched) {
			const marshalled = facet.marshalValue(patchRecord[field]);
			if (marshalled === undefined) {
				/**
				 * `undefined` (and anything else the converter drops)
				 * removes the attribute, matching a put whose model
				 * omitted the field.
				 */
				addRemove(String(field));
			} else {
				addSet(String(field), marshalled);
			}
		}

		for (const target of affected) {
			addSet(target.attributeName, {
				S: target.build(effectiveModel as Partial<T>),
			});
		}

		/**
		 * Mirror `Facet.in`'s double-write: the raw TTL field was
		 * handled above as a plain field; the synthetic epoch-seconds
		 * `ttl` attribute is written (or removed) separately.
		 */
		if (facet.ttl !== undefined && touched.has(facet.ttl)) {
			const ttlValue = normalizeTtl(patchRecord[facet.ttl]);
			if (ttlValue === undefined) {
				addRemove('ttl');
			} else {
				addSet('ttl', { N: ttlValue.toString() });
			}
		}

		const updateParts: string[] = [];
		if (setClauses.length > 0) {
			updateParts.push(`SET ${setClauses.join(', ')}`);
		}
		if (removeClauses.length > 0) {
			updateParts.push(`REMOVE ${removeClauses.join(', ')}`);
		}

		/**
		 * Compose the condition: the never-upsert guard, one guard per
		 * unverified key input, and the caller's own condition. The
		 * placeholder prefixes (`#PK_GUARD`, `#G_`, `#C_`, `#U_`) are
		 * disjoint, so the shared name/value maps can't collide.
		 */
		const conditionParts: string[] = ['attribute_exists (#PK_GUARD)'];
		names['#PK_GUARD'] = 'PK';

		let guardPlaceholder = 0;
		const addEqualityGuard = (
			attributeName: string,
			value: AttributeValue,
		): void => {
			const id = guardPlaceholder++;
			names[`#G_${id}`] = attributeName;
			values[`:G_${id}`] = value;
			conditionParts.push(`#G_${id} = :G_${id}`);
		};
		const addNotExistsGuard = (attributeName: string): void => {
			const id = guardPlaceholder++;
			names[`#G_${id}`] = attributeName;
			conditionParts.push(`attribute_not_exists (#G_${id})`);
		};

		for (const [field, input] of resolved) {
			if (input.source === 'hint') {
				if (input.value === undefined) {
					addNotExistsGuard(String(field));
				} else if (contributesToKeys(input.value)) {
					const marshalled = facet.marshalValue(input.value);
					/* v8 ignore next 3 -- contributesToKeys values always marshal */
					if (marshalled === undefined) {
						continue;
					}
					addEqualityGuard(String(field), marshalled);
				}
			} else if (input.source === 'read' && readItem) {
				// The index signature lies for absent attributes.
				const raw = readItem[String(field)] as AttributeValue | undefined;
				if (raw === undefined) {
					addNotExistsGuard(String(field));
				} else if (
					raw.S !== undefined ||
					raw.N !== undefined ||
					raw.BOOL !== undefined
				) {
					/**
					 * Only scalar attributes can contribute to a key
					 * recompute; guarding document or set attributes
					 * would add spurious failures without protecting
					 * any key.
					 */
					addEqualityGuard(String(field), raw);
				}
			}
		}

		if (options.condition) {
			const compiled = condition(options.condition);
			Object.assign(names, compiled.names);
			Object.assign(values, compiled.values);
			conditionParts.push(`(${compiled.expression})`);
		}

		const updateInput: UpdateItemInput = {
			TableName: facet.connection.tableName,
			Key: key,
			UpdateExpression: updateParts.join(' '),
			ConditionExpression: conditionParts.join(' AND '),
			ExpressionAttributeNames: names,
			ReturnValues: 'ALL_NEW',
			ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
		};
		if (Object.keys(values).length > 0) {
			updateInput.ExpressionAttributeValues = values;
		}

		const result = await facet.connection.dynamoDb.updateItem(updateInput);
		/* v8 ignore next 6 -- ALL_NEW always returns Attributes on success */
		if (!result.Attributes) {
			return {
				wasSuccessful: false,
				error: new Error('UpdateItem returned no attributes.'),
				usedFallbackRead,
			};
		}
		return {
			wasSuccessful: true,
			record: facet.out(result.Attributes),
			usedFallbackRead,
		};
	} catch (error) {
		const failure: PatchFailure<T> = {
			wasSuccessful: false,
			error,
			usedFallbackRead,
		};
		if (error instanceof ConditionalCheckFailedException && error.Item) {
			failure.conflictingItemRaw = error.Item;
			try {
				failure.conflictingItem = facet.out(error.Item);
			} catch {
				// The raw item stays available on conflictingItemRaw.
			}
		}
		return failure;
	}
}
