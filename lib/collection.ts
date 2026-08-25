import type {
	AttributeValue,
	DynamoDB,
	QueryInput,
} from '@aws-sdk/client-dynamodb';
import type { AttributeMap } from './converter/converter.js';
import { decodeCursor, encodeCursor } from './cursor.js';
import {
	filter as buildFilterExpression,
	type FilterConditionExpression,
} from './expression/condition.js';
import type { KeyLayout } from './facet.js';
import { buildKey, Index, IndexKeyNameMap, PK, SK } from './keys.js';

/**
 * The subset of a `Facet` that `collection()` requires from each member.
 * Structural, like `TransactFacet`, so the collection machinery can be
 * exercised without a full facet, and so facets whose `SK` generic is
 * `never` (an empty sort key) still qualify, since private-field
 * invariance makes them unassignable through the nominal `Facet` alias.
 *
 * This carrier is internal to the library: it is not exported from the
 * package root, and annotating a member map with it would erase every
 * model type to `unknown`. Pass concrete facets and let TypeScript
 * infer.
 */
export interface CollectionMemberFacet {
	readonly name: string;
	readonly delimiter: string;
	readonly connection: {
		dynamoDb: Pick<DynamoDB, 'query'>;
		tableName: string;
	};
	readonly pkLayout: KeyLayout;
	readonly skLayout: KeyLayout;
	out(record: AttributeMap): unknown;
	pk(model: object, shard?: number): string;
}

/**
 * The subset of a facet or facet index that a collection reads keys
 * through: the base facet for base-table collections, the member's
 * `FacetIndex` for GSI collections.
 */
interface CollectionKeySource {
	readonly pkLayout: KeyLayout;
	readonly skLayout: KeyLayout;
	pk(model: object, shard?: number): string;
}

const singleBrand = Symbol('faceteer.collection.single');

/**
 * A collection member wrapped by {@link single}, marking it as
 * structurally singular within the partition. See {@link single}.
 */
export interface SingleMember<
	F extends CollectionMemberFacet = CollectionMemberFacet,
> {
	readonly facet: F;
	readonly [singleBrand]: true;
}

/**
 * Marks a collection member as structurally singular: the partition
 * holds at most one record of this facet. In query results the member
 * reads as `Model | undefined` instead of `Model[]`.
 *
 * If a query returns more than one record for a `single()` member, the
 * first record in sort-key order is kept and each extra record lands in
 * the result's `failed` array with an arity error, so one malformed
 * partition can't blank a whole read. The guard spans one query call:
 * `listAll()` enforces it across the whole partition, while paged
 * `list()` can only enforce it within each page, so audit singleton
 * arity through `listAll()`.
 *
 * @example
 * ```ts
 * const pageScreen = collection({
 *   page: single(PageFacet),
 *   post: PostFacet,
 * });
 * ```
 */
export function single<F extends CollectionMemberFacet>(
	facet: F,
): SingleMember<F> {
	return { facet, [singleBrand]: true };
}

function isSingleMember(
	member: CollectionMemberFacet | SingleMember,
): member is SingleMember {
	return singleBrand in member;
}

/**
 * A member of a collection: a facet, or a facet wrapped in
 * {@link single}.
 */
export type CollectionMember = CollectionMemberFacet | SingleMember;

/**
 * The keyed object of facets passed to {@link collection}. The keys
 * become the discriminants (`item.type`, `grouped.<key>`) in query
 * results.
 */
export type MemberMap = Record<string, CollectionMember>;

type OutModel<F> = F extends { out(record: AttributeMap): infer T } ? T : never;

/**
 * The model type of a collection member: unwraps {@link single}, then
 * extracts the model through `out()`'s return position.
 */
export type ModelOf<V> =
	V extends SingleMember<infer F> ? OutModel<F> : OutModel<V>;

/**
 * Whether a collection member was wrapped in {@link single}.
 */
export type IsSingle<V> = V extends SingleMember ? true : false;

/**
 * One row of a collection query result: the member key it dispatched to
 * as `type`, and the validated model as `record`. Discriminates on
 * `type`, so a `switch` narrows `record` to the member's model.
 */
export type TaggedItem<M extends MemberMap> = {
	[K in keyof M]: { type: K; record: ModelOf<M[K]> };
}[keyof M];

/**
 * The per-member view of a collection query result: an array of models
 * for bare members, `Model | undefined` for {@link single} members.
 */
export type Grouped<M extends MemberMap> = {
	[K in keyof M]: M[K] extends SingleMember<infer F>
		? OutModel<F> | undefined
		: ModelOf<M[K]>[];
};

type ModelUnion<M extends MemberMap> = ModelOf<M[keyof M]>;

/**
 * The field names present on every member's model. `keyof` over the
 * union of member models keeps only the shared names.
 */
export type SharedField<M extends MemberMap> = keyof ModelUnion<M>;

type UnwrapMember<V> = V extends SingleMember<infer F> ? F : V;

/**
 * The key source a collection member reads layouts through: the facet
 * itself on the base table, or the member's registered `FacetIndex`
 * when the collection queries a GSI. A member without the index
 * registered resolves to `never`, so it contributes nothing to the
 * compile-time checks; construction throws for it at runtime.
 */
type KeySourceOf<V, I extends Index | undefined> = [I] extends [undefined]
	? UnwrapMember<V>
	: UnwrapMember<V> extends Record<NonNullable<I>, infer S>
		? S
		: never;

/**
 * The `[never]` guards matter: `never extends X` always takes the true
 * branch, which would infer the key fields of an unregistered index
 * member as `unknown` and wave every field through the checks.
 */
type PkFieldsOf<V, I extends Index | undefined> = [KeySourceOf<V, I>] extends [
	never,
]
	? never
	: KeySourceOf<V, I> extends { readonly pkLayout: KeyLayout<infer K> }
		? K
		: never;

type SkFieldsOf<V, I extends Index | undefined> = [KeySourceOf<V, I>] extends [
	never,
]
	? never
	: KeySourceOf<V, I> extends { readonly skLayout: KeyLayout<infer K> }
		? K
		: never;

/**
 * The union of every member's partition-key field names: the fields a
 * collection `query()` must supply.
 */
type RequiredPartitionField<
	M extends MemberMap,
	I extends Index | undefined,
> = {
	[P in keyof M]: PkFieldsOf<M[P], I>;
}[keyof M];

/**
 * The argument type of a collection `query()`: every member's
 * partition-key fields are required, and the remaining fields the
 * member models share stay optional. On a GSI collection (`I` is an
 * `Index`) the required fields come from each member's index
 * partition-key layout instead. A member typed without literal key
 * layouts (a hand-built structural stand-in) contributes nothing
 * required; the query-time partition-key guard is the runtime
 * backstop.
 */
export type PartitionInput<
	M extends MemberMap,
	I extends Index | undefined = undefined,
> = {
	[K in RequiredPartitionField<M, I>]: FieldTypeUnion<M, K>;
} & {
	[
		K in Exclude<SharedField<M>, RequiredPartitionField<M, I>>
	]?: ModelUnion<M>[K];
};

type FieldOn<V, K extends PropertyKey> = K extends keyof ModelOf<V>
	? ModelOf<V>[K]
	: never;

type FieldTypeUnion<M extends MemberMap, K extends PropertyKey> = {
	[P in keyof M]: FieldOn<M[P], K>;
}[keyof M];

/**
 * `true` when every member types the field `K` identically. The union
 * of the per-member types must be assignable back to each member's
 * type; a union widened by any mismatched member fails the check.
 */
type IdenticalAcross<M extends MemberMap, K extends PropertyKey> = {
	[P in keyof M]: [FieldTypeUnion<M, K>] extends [FieldOn<M[P], K>]
		? true
		: false;
}[keyof M] extends true
	? true
	: false;

/**
 * `true` when the field `K` appears in every member's sort key: on the
 * base table, or on the `I` index for a GSI collection.
 */
type InEverySortKey<
	M extends MemberMap,
	K extends PropertyKey,
	I extends Index | undefined,
> = {
	[P in keyof M]: [K] extends [SkFieldsOf<M[P], I>] ? true : false;
}[keyof M] extends true
	? true
	: false;

/**
 * The fields eligible as an ordered collection's `orderBy` axis: shared
 * by every member, part of every member's sort key (on the base table,
 * or on the `I` index), typed identically on every member, and `Date`
 * or `string`. `number` fields are excluded because composite keys
 * stringify numbers without zero-padding, so lexicographic order
 * diverges from numeric order. Whether the field leads each sort key
 * isn't visible in the types, because key order is a runtime value, so
 * construction verifies the leading position and throws otherwise.
 */
export type AxisField<
	M extends MemberMap,
	I extends Index | undefined = undefined,
> = {
	[K in SharedField<M>]: InEverySortKey<M, K, I> extends true
		? IdenticalAcross<M, K> extends true
			? [FieldTypeUnion<M, K>] extends [Date]
				? K
				: [FieldTypeUnion<M, K>] extends [string]
					? K
					: never
			: never
		: never;
}[SharedField<M>];

/**
 * The value type of an ordered collection's axis field.
 */
export type AxisValue<
	M extends MemberMap,
	A extends PropertyKey,
> = FieldTypeUnion<M, A>;

/**
 * A sort-key bound on an ordered collection: a plain value of the axis
 * field's type, or a raw key string. A string bound that starts with
 * the members' shared sort-key prefix (followed by the delimiter, or
 * standing alone) is used verbatim. That is the escape hatch for
 * passing a member's `sk({...})` output or a hand-built key. Any other
 * value is composed into a key with the shared prefix and delimiter
 * through the same `buildKey` path the write side uses.
 */
export type AxisBound<M extends MemberMap, A extends PropertyKey> =
	AxisValue<M, A> | string;

/**
 * How a collection query result reports each returned row.
 */
export interface CollectionFailure<M extends MemberMap> {
	/** The raw item, as DynamoDB returned it. */
	item: AttributeMap;
	/** The member key the row dispatched to. */
	facet: keyof M;
	/** The validator error, or an arity error for extra `single()` rows. */
	error: unknown;
}

/**
 * The result of a collection query.
 */
export interface CollectionResult<M extends MemberMap> {
	/**
	 * Every dispatched record, interleaved in sort-key order, tagged
	 * with the member key it belongs to.
	 */
	records: TaggedItem<M>[];
	/**
	 * The same records grouped per member key: arrays for bare members,
	 * `Model | undefined` for `single()` members. Every key is present
	 * even when the partition holds no record for it. With paged
	 * `list()` this reflects the current page only; use `listAll()`
	 * when the read is "the whole aggregate".
	 */
	grouped: Grouped<M>;
	/**
	 * Pagination cursor when more rows remain. Never set by
	 * `listAll()`, `latest()`, or `earliest()`.
	 */
	cursor?: string;
	/**
	 * Rows whose `facet` attribute matched no member (foreign entity
	 * types sharing the partition, or records written before the
	 * `facet` attribute existed). Set `onUnknown: 'throw'` to reject
	 * instead of collecting.
	 */
	unmatched: AttributeMap[];
	/**
	 * Rows that dispatched to a member but failed its validator (with
	 * `onInvalid: 'collect'`), plus extra rows for `single()` members.
	 */
	failed: CollectionFailure<M>[];
}

/**
 * Options accepted by collection query terminals.
 */
export interface CollectionQueryOptions<M extends MemberMap> {
	/**
	 * The cursor returned by a previous page of the same query.
	 */
	cursor?: string;
	/**
	 * The maximum number of rows DynamoDB evaluates for the page.
	 * Unmatched and failed rows count against it, so it is not the
	 * number of dispatched records.
	 */
	limit?: number;
	/**
	 * A DynamoDB `FilterExpression` over the fields the member models
	 * share. Filters run after the page is cut, so a filtered page can
	 * be short or empty while matching rows remain. Paginate on cursor
	 * presence, not on page emptiness.
	 */
	filter?: FilterConditionExpression<Pick<ModelUnion<M>, SharedField<M>>>;
	/**
	 * Use a strongly consistent read. Only valid on base-table
	 * collections; a GSI collection query rejects it at runtime,
	 * because DynamoDB cannot serve consistent reads from a global
	 * secondary index.
	 */
	consistentRead?: boolean;
	/**
	 * What to do with rows whose `facet` attribute matches no member.
	 * `'collect'` (the default) places them in the result's
	 * `unmatched` array; `'throw'` rejects the query.
	 */
	onUnknown?: 'collect' | 'throw';
	/**
	 * What to do with rows that match a member but fail its validator.
	 * `'throw'` (the default) matches the library's validate-on-read
	 * philosophy; `'collect'` places the failure in the result's
	 * `failed` array and continues, so one bad row can't blank an
	 * entire screen load.
	 */
	onInvalid?: 'throw' | 'collect';
}

/**
 * The traversal direction of an ordered collection query. Ascending
 * follows the axis from earliest to latest.
 */
export type CollectionDirection = 'ascending' | 'descending';

/**
 * Options accepted by ordered collection query terminals, which add a
 * traversal `direction` to the common options.
 */
export interface OrderedCollectionQueryOptions<
	M extends MemberMap,
> extends CollectionQueryOptions<M> {
	/**
	 * The traversal direction along the axis. Defaults to
	 * `'ascending'`.
	 */
	direction?: CollectionDirection;
}

/**
 * Construction options for an ordered collection. `I` captures the
 * literal type of `index` so GSI collections can gate `consistentRead`
 * off at compile time.
 */
export interface OrderedCollectionOptions<
	A,
	I extends Index | undefined = Index | undefined,
> {
	/**
	 * The model field the collection orders on. Every member's sort key
	 * (on the base table, or on `index` when given) must lead with this
	 * field; construction verifies it and throws otherwise.
	 */
	orderBy: A;
	/**
	 * Query the collection through this GSI instead of the base table.
	 * Every member must have registered the index with `addIndex`.
	 */
	index?: I;
}

/**
 * DynamoDB rejects `ConsistentRead` on queries against a global
 * secondary index, so `consistentRead` is only offered when the
 * collection reads the base table. Intersected into the ordered query
 * methods' option types, which omit the inherited `consistentRead` so
 * this intersection controls the property: on base-table collections
 * (`I` is `undefined`) it stays a boolean, and on GSI collections its
 * type is a message literal, so passing `true` fails to compile with
 * the reason in the error text. The runtime rejection in
 * `buildQueryInput` backstops untyped callers.
 */
type ConsistentReadOnBaseCollection<I extends Index | undefined> = [I] extends [
	undefined,
]
	? {
			/**
			 * Use a strongly consistent read. See
			 * {@link CollectionQueryOptions.consistentRead}.
			 */
			consistentRead?: boolean;
		}
	: {
			/**
			 * Not available: DynamoDB cannot serve consistent reads from a
			 * global secondary index.
			 */
			consistentRead?: 'consistentRead is unavailable on a GSI-backed collection';
		};

/**
 * The verified shared axis of an ordered collection.
 */
interface CollectionAxis {
	readonly orderBy: PropertyKey;
	readonly prefix: string;
	readonly delimiter: string;
}

interface MemberRuntime {
	readonly key: string;
	readonly single: boolean;
	readonly facet: CollectionMemberFacet;
	readonly keySource: CollectionKeySource;
}

interface CollectionCore {
	readonly members: readonly MemberRuntime[];
	readonly byName: ReadonlyMap<string, MemberRuntime>;
	readonly dynamoDb: Pick<DynamoDB, 'query'>;
	readonly tableName: string;
	readonly index?: Index;
	readonly pkAttribute: string;
	readonly skAttribute: string;
}

/**
 * Group facets that share a partition into one cross-entity query
 * surface, the read half of the item-collection pattern. One DynamoDB
 * `Query` on the shared partition returns every member's records, each
 * dispatched to its own validator and type by the `facet` attribute
 * the write side already stamps.
 *
 * The keys of the `facets` object become the discriminants in query
 * results (`item.type`, `grouped.<key>`). Wrap structurally singular
 * members in {@link single}.
 *
 * Without options this builds a default collection: members may have
 * any sort-key layouts, and the query surface is `list`, `listAll`,
 * and `first`. Range operators don't exist on this kind, because rows
 * cluster by each facet's sort-key prefix and a range over them would
 * return a plausible-looking, silently wrong subset.
 *
 * With `{ orderBy }` this builds an ordered collection: `orderBy`
 * offers only the fields shared by every member, present in every
 * member's sort key, and typed `Date` or `string`, and construction
 * verifies that each sort key leads with the named field on a shared
 * prefix and delimiter, which makes the partition's sort
 * order meaningful across types and unlocks `between`, `equals`, the
 * four comparisons, `latest`, `earliest`, and a `direction` option,
 * all taking plain values of the axis field's type. With `index` the
 * axis is verified against each member's GSI layout instead, and
 * queries run against that index.
 *
 * Construction throws when members disagree on the DynamoDB client
 * instance or table name, when two members share a facet name, or when
 * an ordered member's layout doesn't fit the axis.
 *
 * @example
 * ```ts
 * // Default kind: any sort-key layouts.
 * const pageScreen = collection({
 *   page: single(PageFacet),
 *   settings: single(SettingsFacet),
 *   post: PostFacet,
 * });
 * const screen = await pageScreen.query({ pageId: 'p1' }).listAll();
 * screen.grouped.page?.pageName; // Page | undefined
 * screen.grouped.post;           // Post[]
 *
 * // Ordered kind: every member's SK leads with createdAt.
 * const feed = collection(
 *   { comment: CommentFacet, edit: EditFacet },
 *   { orderBy: 'createdAt' },
 * );
 * const { records } = await feed
 *   .query({ postId: 'post-7' })
 *   .between(new Date('2024-01-01'), new Date('2024-02-01'));
 * ```
 */
export function collection<M extends MemberMap>(facets: M): FacetCollection<M>;
export function collection<
	M extends MemberMap,
	A extends AxisField<M, I>,
	I extends Index | undefined = undefined,
>(
	facets: M,
	options: OrderedCollectionOptions<A, I>,
): OrderedFacetCollection<M, A, I>;
export function collection<
	M extends MemberMap,
	A extends AxisField<M, I>,
	I extends Index | undefined,
>(
	facets: M,
	options?: OrderedCollectionOptions<A, I>,
): FacetCollection<M> | OrderedFacetCollection<M, A, I> {
	const index = options?.index;
	const members = buildMembers(facets, index);

	const first = members[0];
	for (const member of members) {
		if (member.facet.connection.dynamoDb !== first.facet.connection.dynamoDb) {
			throw new Error(
				`Collection members "${first.key}" and "${member.key}" use different DynamoDB client instances. Every member of a collection must share the same client.`,
			);
		}
		if (
			member.facet.connection.tableName !== first.facet.connection.tableName
		) {
			throw new Error(
				`Collection members "${first.key}" (table "${first.facet.connection.tableName}") and "${member.key}" (table "${member.facet.connection.tableName}") use different tables. Every member of a collection must share the same table.`,
			);
		}
	}

	const byName = new Map<string, MemberRuntime>();
	for (const member of members) {
		const existing = byName.get(member.facet.name);
		if (existing) {
			throw new Error(
				`Collection members "${existing.key}" and "${member.key}" share the facet name "${member.facet.name}". The facet name routes query results, so every member needs a unique one.`,
			);
		}
		byName.set(member.facet.name, member);
	}

	const keyNames = index ? IndexKeyNameMap[index] : { PK, SK };
	const core: CollectionCore = {
		members,
		byName,
		dynamoDb: first.facet.connection.dynamoDb,
		tableName: first.facet.connection.tableName,
		index,
		pkAttribute: keyNames.PK,
		skAttribute: keyNames.SK,
	};

	return options
		? new OrderedFacetCollection<M, A, I>(
				core,
				verifyAxis(members, options.orderBy, index),
			)
		: new FacetCollection<M>(core);
}

function buildMembers(facets: MemberMap, index?: Index): MemberRuntime[] {
	const keys = Object.keys(facets);
	if (keys.length === 0) {
		throw new Error('collection() requires at least one member facet.');
	}
	return keys.map((key) => {
		const value = facets[key];
		const isSingle = isSingleMember(value);
		const facet = isSingle ? value.facet : value;
		let keySource: CollectionKeySource = facet;
		if (index) {
			const accessor = (facet as unknown as Record<string, unknown>)[index];
			if (!isKeySource(accessor)) {
				throw new Error(
					`Collection member "${key}" has no ${index} index registered. Register the index with addIndex on every member before declaring a collection on it.`,
				);
			}
			keySource = accessor;
		}
		return { key, single: isSingle, facet, keySource };
	});
}

function isKeySource(value: unknown): value is CollectionKeySource {
	if (typeof value !== 'object' || value === null) {
		return false;
	}
	const candidate = value as Partial<Record<'pk' | 'skLayout', unknown>>;
	return (
		typeof candidate.pk === 'function' &&
		typeof candidate.skLayout === 'object' &&
		candidate.skLayout !== null
	);
}

function verifyAxis(
	members: readonly MemberRuntime[],
	orderBy: PropertyKey,
	index?: Index,
): CollectionAxis {
	const where = index ? `${index} sort key` : 'sort key';
	const firstLayout = members[0].keySource.skLayout;
	for (const member of members) {
		const layout = member.keySource.skLayout;
		if (layout.sharded) {
			throw new Error(
				`Collection member "${member.key}" has a sharded ${where}. Shard ids bucket the sort order, so an ordered collection can't include it.`,
			);
		}
		if (layout.keys[0] !== orderBy) {
			const found =
				layout.keys.length > 0
					? `leads with "${String(layout.keys[0])}"`
					: 'has no sort-key fields';
			throw new Error(
				`Collection member "${member.key}" doesn't sort on the "${String(orderBy)}" axis: its ${where} ${found}. Every member of an ordered collection must lead its sort key with the axis field.`,
			);
		}
		if (layout.prefix !== firstLayout.prefix) {
			throw new Error(
				`Collection members "${members[0].key}" (prefix "${firstLayout.prefix}") and "${member.key}" (prefix "${layout.prefix}") use different ${where} prefixes. An ordered collection needs one shared prefix so the axis occupies a single contiguous band.`,
			);
		}
		if (layout.delimiter !== firstLayout.delimiter) {
			throw new Error(
				`Collection members "${members[0].key}" (delimiter "${firstLayout.delimiter}") and "${member.key}" (delimiter "${layout.delimiter}") use different key delimiters. An ordered collection needs one shared delimiter.`,
			);
		}
	}
	return {
		orderBy,
		prefix: firstLayout.prefix,
		delimiter: firstLayout.delimiter,
	};
}

/**
 * Compute the shared partition key, verifying that every member builds
 * the same string. Two facets can share partition-key fields yet build
 * different strings from different prefixes; this guard needs the
 * partition value, so it runs at query time.
 */
function resolvePartitionKey(
	core: CollectionCore,
	partition: object,
	shard?: number,
): string {
	/**
	 * `buildKey` silently omits undefined fields, so a missing
	 * partition-key field would build a truncated key that every member
	 * agrees on, defeat the cross-member comparison below, and read the
	 * wrong partition without an error. Require every member's
	 * partition-key fields up front.
	 */
	const partitionFields = partition as Record<PropertyKey, unknown>;
	for (const member of core.members) {
		for (const field of member.keySource.pkLayout.keys) {
			if (partitionFields[field] === undefined) {
				throw new Error(
					`Collection member "${member.key}" builds its partition key from "${String(field)}", which this query() did not supply. A missing field would silently build a truncated partition key, so every member's partition-key fields are required.`,
				);
			}
		}
	}

	const first = core.members[0];
	const expected = first.keySource.pk(partition, shard);
	for (const member of core.members) {
		const actual = member.keySource.pk(partition, shard);
		if (actual !== expected) {
			throw new Error(
				`Collection members "${first.key}" and "${member.key}" build different partition keys for this query: "${expected}" versus "${actual}". Every member of a collection must resolve the same partition-key string.`,
			);
		}
	}
	return expected;
}

/**
 * Dispatches raw query rows to their owning members and accumulates
 * the two result views in one O(n) pass.
 */
class Dispatcher<M extends MemberMap> {
	readonly records: TaggedItem<M>[] = [];
	readonly grouped: Grouped<M>;
	readonly unmatched: AttributeMap[] = [];
	readonly failed: CollectionFailure<M>[] = [];
	readonly #singleSeen = new Set<string>();
	readonly #byName: ReadonlyMap<string, MemberRuntime>;
	readonly #onUnknown: 'collect' | 'throw';
	readonly #onInvalid: 'throw' | 'collect';

	constructor(core: CollectionCore, options: RunnerOptions<M>) {
		this.#byName = core.byName;
		this.#onUnknown = options.onUnknown ?? 'collect';
		this.#onInvalid = options.onInvalid ?? 'throw';
		const grouped: Record<string, unknown> = {};
		for (const member of core.members) {
			grouped[member.key] = member.single ? undefined : [];
		}
		this.grouped = grouped as Grouped<M>;
	}

	/**
	 * Route one raw item. Returns `true` when the item became a record.
	 */
	dispatch(item: AttributeMap): boolean {
		/**
		 * `AttributeMap`'s index signature types every attribute as
		 * present, but legacy rows written before the `facet` attribute
		 * existed genuinely lack it.
		 */
		const facetAttribute = item.facet as AttributeValue | undefined;
		const facetName = facetAttribute?.S;
		const member =
			facetName === undefined ? undefined : this.#byName.get(facetName);
		if (!member) {
			if (this.#onUnknown === 'throw') {
				throw new Error(
					facetName === undefined
						? 'Collection query returned a row with no facet attribute, and onUnknown is set to "throw".'
						: `Collection query returned a row whose facet "${facetName}" matches no member, and onUnknown is set to "throw".`,
				);
			}
			this.unmatched.push(item);
			return false;
		}

		let record: unknown;
		try {
			record = member.facet.out(item);
		} catch (error) {
			if (this.#onInvalid === 'throw') {
				throw error;
			}
			this.failed.push({ item, facet: member.key, error });
			return false;
		}

		if (member.single) {
			if (this.#singleSeen.has(member.key)) {
				this.failed.push({
					item,
					facet: member.key,
					error: new Error(
						`Collection member "${member.key}" is declared single() but the partition holds more than one of its records. The first record in traversal order was kept.`,
					),
				});
				return false;
			}
			this.#singleSeen.add(member.key);
			(this.grouped as Record<string, unknown>)[member.key] = record;
		} else {
			(this.grouped as Record<string, unknown[]>)[member.key].push(record);
		}

		this.records.push({
			type: member.key,
			record,
		} as TaggedItem<M>);
		return true;
	}

	result(cursor?: string): CollectionResult<M> {
		const result: CollectionResult<M> = {
			records: this.records,
			grouped: this.grouped,
			unmatched: this.unmatched,
			failed: this.failed,
		};
		if (cursor !== undefined) {
			result.cursor = cursor;
		}
		return result;
	}
}

interface SortCondition {
	/** Key-condition clause referencing `#SK`, such as `#SK = :sort`. */
	clause: string;
	values: Record<string, AttributeValue>;
}

interface ExecOptions {
	cursor?: string;
	limit?: number;
	scanForward: boolean;
	filter?: FilterConditionExpression<never>;
	consistentRead?: boolean | string;
}

/**
 * The options shape the runners consume. `consistentRead` widens to
 * `boolean | string` because the GSI-gated option types admit a
 * message-literal string; any truthy value on an index query hits the
 * runtime rejection in `buildQueryInput`.
 */
interface RunnerOptions<M extends MemberMap> extends Omit<
	CollectionQueryOptions<M>,
	'consistentRead'
> {
	consistentRead?: boolean | string;
}

function buildQueryInput(
	core: CollectionCore,
	partitionKey: string,
	sort: SortCondition | undefined,
	exec: ExecOptions,
): QueryInput {
	const names: Record<string, string> = { '#PK': core.pkAttribute };
	const values: Record<string, AttributeValue> = {
		':partition': { S: partitionKey },
	};
	let keyCondition = '#PK = :partition';
	if (sort) {
		names['#SK'] = core.skAttribute;
		Object.assign(values, sort.values);
		keyCondition = `#PK = :partition AND ${sort.clause}`;
	}

	const input: QueryInput = {
		TableName: core.tableName,
		IndexName: core.index,
		KeyConditionExpression: keyCondition,
		ExpressionAttributeNames: names,
		ExpressionAttributeValues: values,
		Limit: exec.limit,
		ScanIndexForward: exec.scanForward,
	};

	if (exec.consistentRead) {
		if (core.index) {
			throw new Error(
				`Consistent reads are not supported on global secondary indexes; remove the consistentRead option from this ${core.index} collection query.`,
			);
		}
		input.ConsistentRead = true;
	}

	if (exec.cursor) {
		input.ExclusiveStartKey = decodeCursor(exec.cursor);
	}

	if (exec.filter) {
		const compiled = buildFilterExpression(exec.filter);
		input.FilterExpression = compiled.expression;
		Object.assign(names, compiled.names);
		Object.assign(values, compiled.values);
	}

	return input;
}

async function runSinglePage<M extends MemberMap>(
	core: CollectionCore,
	partitionKey: string,
	sort: SortCondition | undefined,
	options: RunnerOptions<M>,
	scanForward: boolean,
): Promise<CollectionResult<M>> {
	const dispatcher = new Dispatcher<M>(core, options);
	const input = buildQueryInput(core, partitionKey, sort, {
		cursor: options.cursor,
		limit: options.limit,
		scanForward,
		filter: options.filter,
		consistentRead: options.consistentRead,
	});
	const response = await core.dynamoDb.query(input);
	for (const item of response.Items ?? []) {
		dispatcher.dispatch(item);
	}
	return dispatcher.result(
		response.LastEvaluatedKey
			? encodeCursor(response.LastEvaluatedKey)
			: undefined,
	);
}

async function runAllPages<M extends MemberMap>(
	core: CollectionCore,
	partitionKey: string,
	sort: SortCondition | undefined,
	options: RunnerOptions<M>,
	scanForward: boolean,
): Promise<CollectionResult<M>> {
	const dispatcher = new Dispatcher<M>(core, options);
	let lastKey: Record<string, AttributeValue> | undefined;
	do {
		const input = buildQueryInput(core, partitionKey, sort, {
			scanForward,
			filter: options.filter,
			consistentRead: options.consistentRead,
		});
		if (lastKey) {
			input.ExclusiveStartKey = lastKey;
		}
		const response = await core.dynamoDb.query(input);
		for (const item of response.Items ?? []) {
			dispatcher.dispatch(item);
		}
		lastKey = response.LastEvaluatedKey;
	} while (lastKey);
	return dispatcher.result();
}

async function runFirst<M extends MemberMap>(
	core: CollectionCore,
	partitionKey: string,
	options: RunnerOptions<M>,
	scanForward: boolean,
): Promise<TaggedItem<M> | null> {
	const dispatcher = new Dispatcher<M>(core, options);
	let lastKey: Record<string, AttributeValue> | undefined;
	/**
	 * The first page evaluates a single row, so the common case (the
	 * partition leads with a member record) reads the minimum. Each
	 * empty page doubles the size, so a partition that leads with many
	 * foreign or filtered rows costs O(log n) round trips instead of
	 * one per row.
	 */
	let pageLimit = 1;
	do {
		const input = buildQueryInput(core, partitionKey, undefined, {
			limit: pageLimit,
			scanForward,
			filter: options.filter,
			consistentRead: options.consistentRead,
		});
		if (lastKey) {
			input.ExclusiveStartKey = lastKey;
		}
		const response = await core.dynamoDb.query(input);
		for (const item of response.Items ?? []) {
			if (dispatcher.dispatch(item)) {
				return dispatcher.records[0];
			}
		}
		pageLimit = Math.min(pageLimit * 2, 100);
		lastKey = response.LastEvaluatedKey;
	} while (lastKey);
	return null;
}

async function runBounded<M extends MemberMap>(
	core: CollectionCore,
	partitionKey: string,
	count: number,
	descending: boolean,
	options: RunnerOptions<M>,
	label: 'latest' | 'earliest',
): Promise<CollectionResult<M>> {
	if (!Number.isInteger(count) || count < 1) {
		throw new Error(
			`${label}() needs a positive integer count; received ${count}.`,
		);
	}
	const dispatcher = new Dispatcher<M>(core, options);
	let lastKey: Record<string, AttributeValue> | undefined;
	do {
		/**
		 * Every page asks for `count` rows rather than the shrinking
		 * remainder, so partitions heavy with unmatched or filtered rows
		 * don't degenerate into single-item queries. Dispatch stops the
		 * moment the count is reached; rows past the cutoff sort beyond
		 * the returned window and are ignored.
		 */
		const input = buildQueryInput(core, partitionKey, undefined, {
			limit: count,
			scanForward: !descending,
			filter: options.filter,
			consistentRead: options.consistentRead,
		});
		if (lastKey) {
			input.ExclusiveStartKey = lastKey;
		}
		const response = await core.dynamoDb.query(input);
		for (const item of response.Items ?? []) {
			dispatcher.dispatch(item);
			if (dispatcher.records.length >= count) {
				return dispatcher.result();
			}
		}
		lastKey = response.LastEvaluatedKey;
	} while (lastKey);
	return dispatcher.result();
}

/**
 * A cross-entity collection over facets with arbitrary sort-key
 * layouts. Built by {@link collection} without options. Rows cluster
 * by each facet's sort-key prefix, so the partition's sort order has
 * no cross-type meaning: the query surface is `list`, `listAll`, and
 * `first`, with no range operators and no direction.
 */
export class FacetCollection<M extends MemberMap> {
	#core: CollectionCore;

	/**
	 * @internal Built by {@link collection}; construct through it.
	 */
	constructor(core: CollectionCore) {
		this.#core = core;
	}

	/**
	 * Begin a query over the shared partition.
	 *
	 * @param partition - The fields that compose the members' partition
	 * keys; every member's partition-key fields are required at compile
	 * time. The builder computes the partition string through every
	 * member and throws if any differ.
	 * @param shard - Explicit shard id when the members' partition keys
	 * are sharded.
	 */
	query(partition: PartitionInput<M>, shard?: number): BaseCollectionQuery<M> {
		return new BaseCollectionQuery<M>(
			this.#core,
			resolvePartitionKey(this.#core, partition, shard),
		);
	}
}

/**
 * A cross-entity collection whose members all sort on one declared
 * axis field. Built by {@link collection} with `{ orderBy }`. Adds the
 * value-typed range vocabulary to the query surface.
 */
export class OrderedFacetCollection<
	M extends MemberMap,
	A extends AxisField<M, I>,
	I extends Index | undefined = undefined,
> {
	#core: CollectionCore;
	#axis: CollectionAxis;

	/**
	 * @internal Built by {@link collection}; construct through it.
	 */
	constructor(core: CollectionCore, axis: CollectionAxis) {
		this.#core = core;
		this.#axis = axis;
	}

	/**
	 * Begin a query over the shared partition.
	 *
	 * @param partition - The fields that compose the members' partition
	 * keys; every member's partition-key fields are required at compile
	 * time. The builder computes the partition string through every
	 * member and throws if any differ.
	 * @param shard - Explicit shard id when the members' partition keys
	 * are sharded.
	 */
	query(
		partition: PartitionInput<M, I>,
		shard?: number,
	): OrderedCollectionQuery<M, A, I> {
		return new OrderedCollectionQuery<M, A, I>(
			this.#core,
			resolvePartitionKey(this.#core, partition, shard),
			this.#axis,
		);
	}
}

/**
 * The query surface of a default collection: the whole partition, with
 * no cross-type range operators.
 */
export class BaseCollectionQuery<M extends MemberMap> {
	#core: CollectionCore;
	#partitionKey: string;

	/**
	 * @internal Built by a collection's `query()`; construct through it.
	 */
	constructor(core: CollectionCore, partitionKey: string) {
		this.#core = core;
		this.#partitionKey = partitionKey;
	}

	/**
	 * Fetch one page of the partition, every member type interleaved in
	 * sort-key order.
	 *
	 * @returns A {@link CollectionResult} whose `grouped` view reflects
	 * this page only, with a `cursor` when more rows remain.
	 */
	async list(
		options: CollectionQueryOptions<M> = {},
	): Promise<CollectionResult<M>> {
		return runSinglePage(
			this.#core,
			this.#partitionKey,
			undefined,
			options,
			true,
		);
	}

	/**
	 * Drain every page of the partition before dispatching, so the
	 * result reflects the whole aggregate. The result carries no
	 * cursor.
	 */
	async listAll(
		options: Omit<CollectionQueryOptions<M>, 'cursor' | 'limit'> = {},
	): Promise<CollectionResult<M>> {
		return runAllPages(
			this.#core,
			this.#partitionKey,
			undefined,
			options,
			true,
		);
	}

	/**
	 * Fetch the first member record in the partition, or `null` when
	 * the partition holds none.
	 *
	 * Pages forward while a page yields zero member records and a
	 * cursor remains, so a leading unmatched row (or, with
	 * `onInvalid: 'collect'`, a leading invalid row) doesn't produce a
	 * false `null`. The paging is uncapped and terminates at the end of
	 * the partition.
	 */
	async first(
		options: Omit<CollectionQueryOptions<M>, 'cursor' | 'limit'> = {},
	): Promise<TaggedItem<M> | null> {
		return runFirst(this.#core, this.#partitionKey, options, true);
	}
}

/**
 * The query surface of an ordered collection: everything the default
 * kind offers, plus value-typed range operators, `latest`, `earliest`,
 * and a traversal `direction`.
 */
export class OrderedCollectionQuery<
	M extends MemberMap,
	A extends AxisField<M, I>,
	I extends Index | undefined = undefined,
> {
	#core: CollectionCore;
	#partitionKey: string;
	#axis: CollectionAxis;

	/**
	 * @internal Built by a collection's `query()`; construct through it.
	 */
	constructor(
		core: CollectionCore,
		partitionKey: string,
		axis: CollectionAxis,
	) {
		this.#core = core;
		this.#partitionKey = partitionKey;
		this.#axis = axis;
	}

	/**
	 * Compose a sort-key bound from an axis value, passing through raw
	 * key strings. See {@link AxisBound}.
	 */
	#bound(value: AxisBound<M, A>): string {
		const axis = this.#axis;
		if (
			typeof value === 'string' &&
			(value === axis.prefix || value.startsWith(axis.prefix + axis.delimiter))
		) {
			return value;
		}
		return buildKey<{ value: AxisBound<M, A> }, 'value'>(
			{ keys: ['value'], prefix: axis.prefix },
			{ value },
			axis.delimiter,
		);
	}

	#scanForward(options: { direction?: CollectionDirection }): boolean {
		return options.direction !== 'descending';
	}

	/**
	 * Fetch one page of the partition, every member type interleaved
	 * along the axis.
	 */
	async list(
		options: Omit<OrderedCollectionQueryOptions<M>, 'consistentRead'> &
			ConsistentReadOnBaseCollection<I> = {},
	): Promise<CollectionResult<M>> {
		return runSinglePage(
			this.#core,
			this.#partitionKey,
			undefined,
			options,
			this.#scanForward(options),
		);
	}

	/**
	 * Drain every page of the partition before dispatching. The result
	 * carries no cursor.
	 */
	async listAll(
		options: Omit<
			OrderedCollectionQueryOptions<M>,
			'consistentRead' | 'cursor' | 'limit'
		> &
			ConsistentReadOnBaseCollection<I> = {},
	): Promise<CollectionResult<M>> {
		return runAllPages(
			this.#core,
			this.#partitionKey,
			undefined,
			options,
			this.#scanForward(options),
		);
	}

	/**
	 * Fetch the first member record along the axis, or `null` when the
	 * partition holds none. With `direction: 'descending'` this is the
	 * latest record. Pages past leading unmatched (and, with
	 * `onInvalid: 'collect'`, invalid) rows.
	 */
	async first(
		options: Omit<
			OrderedCollectionQueryOptions<M>,
			'consistentRead' | 'cursor' | 'limit'
		> &
			ConsistentReadOnBaseCollection<I> = {},
	): Promise<TaggedItem<M> | null> {
		return runFirst(
			this.#core,
			this.#partitionKey,
			options,
			this.#scanForward(options),
		);
	}

	/**
	 * Fetch records whose full sort key equals the composed bound.
	 *
	 * A member whose sort key carries trailing tie-breaker fields after
	 * the axis builds longer keys, so its rows only match `equals` when
	 * the bound is that member's own `sk({...})` output. To fetch every
	 * record at one axis value across members with tie-breakers, use
	 * {@link between} with raw-string bounds: the composed value as the
	 * start, and the composed value plus a `'\uffff'` sentinel as the
	 * end.
	 */
	async equals(
		value: AxisBound<M, A>,
		options: Omit<OrderedCollectionQueryOptions<M>, 'consistentRead'> &
			ConsistentReadOnBaseCollection<I> = {},
	): Promise<CollectionResult<M>> {
		return this.#range('#SK = :sort', { ':sort': this.#bound(value) }, options);
	}

	/**
	 * Fetch records whose sort key is strictly greater than the bound.
	 */
	async greaterThan(
		value: AxisBound<M, A>,
		options: Omit<OrderedCollectionQueryOptions<M>, 'consistentRead'> &
			ConsistentReadOnBaseCollection<I> = {},
	): Promise<CollectionResult<M>> {
		return this.#range('#SK > :sort', { ':sort': this.#bound(value) }, options);
	}

	/**
	 * Fetch records whose sort key is greater than or equal to the
	 * bound.
	 */
	async greaterThanOrEqual(
		value: AxisBound<M, A>,
		options: Omit<OrderedCollectionQueryOptions<M>, 'consistentRead'> &
			ConsistentReadOnBaseCollection<I> = {},
	): Promise<CollectionResult<M>> {
		return this.#range(
			'#SK >= :sort',
			{ ':sort': this.#bound(value) },
			options,
		);
	}

	/**
	 * Fetch records whose sort key is strictly less than the bound.
	 */
	async lessThan(
		value: AxisBound<M, A>,
		options: Omit<OrderedCollectionQueryOptions<M>, 'consistentRead'> &
			ConsistentReadOnBaseCollection<I> = {},
	): Promise<CollectionResult<M>> {
		return this.#range('#SK < :sort', { ':sort': this.#bound(value) }, options);
	}

	/**
	 * Fetch records whose sort key is less than or equal to the bound.
	 */
	async lessThanOrEqual(
		value: AxisBound<M, A>,
		options: Omit<OrderedCollectionQueryOptions<M>, 'consistentRead'> &
			ConsistentReadOnBaseCollection<I> = {},
	): Promise<CollectionResult<M>> {
		return this.#range(
			'#SK <= :sort',
			{ ':sort': this.#bound(value) },
			options,
		);
	}

	/**
	 * Fetch records whose sort key is between the two bounds, inclusive
	 * on both ends. Bounds compare in their composed string form, so a
	 * row at exactly the end value with trailing tie-breaker fields
	 * sorts after the composed end bound and is excluded, matching the
	 * semantics of a single facet's `between`.
	 *
	 * `start` must not exceed `end` regardless of `direction`, which
	 * only sets the traversal order within the window; DynamoDB rejects
	 * a reversed `BETWEEN` with a `ValidationException`.
	 */
	async between(
		start: AxisBound<M, A>,
		end: AxisBound<M, A>,
		options: Omit<OrderedCollectionQueryOptions<M>, 'consistentRead'> &
			ConsistentReadOnBaseCollection<I> = {},
	): Promise<CollectionResult<M>> {
		return this.#range(
			'#SK BETWEEN :start AND :end',
			{ ':start': this.#bound(start), ':end': this.#bound(end) },
			options,
		);
	}

	/**
	 * Fetch the `count` most recent records along the axis, newest
	 * first. Pages until `count` member records are collected or the
	 * partition ends; the result carries no cursor.
	 */
	async latest(
		count: number,
		options: Omit<
			OrderedCollectionQueryOptions<M>,
			'consistentRead' | 'cursor' | 'limit' | 'direction'
		> &
			ConsistentReadOnBaseCollection<I> = {},
	): Promise<CollectionResult<M>> {
		return runBounded(
			this.#core,
			this.#partitionKey,
			count,
			true,
			options,
			'latest',
		);
	}

	/**
	 * Fetch the `count` earliest records along the axis, oldest first.
	 * Pages until `count` member records are collected or the partition
	 * ends; the result carries no cursor.
	 */
	async earliest(
		count: number,
		options: Omit<
			OrderedCollectionQueryOptions<M>,
			'consistentRead' | 'cursor' | 'limit' | 'direction'
		> &
			ConsistentReadOnBaseCollection<I> = {},
	): Promise<CollectionResult<M>> {
		return runBounded(
			this.#core,
			this.#partitionKey,
			count,
			false,
			options,
			'earliest',
		);
	}

	async #range(
		clause: string,
		bounds: Record<string, string>,
		options: RunnerOptions<M> & { direction?: CollectionDirection },
	): Promise<CollectionResult<M>> {
		const values: Record<string, AttributeValue> = {};
		for (const [placeholder, bound] of Object.entries(bounds)) {
			values[placeholder] = { S: bound };
		}
		return runSinglePage(
			this.#core,
			this.#partitionKey,
			{ clause, values },
			options,
			this.#scanForward(options),
		);
	}
}
