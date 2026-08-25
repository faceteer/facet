/*
 * Public API surface of `@faceteer/facet`. Anything not re-exported
 * here is internal and not covered by SemVer. The `exports` field in
 * `package.json` additionally fences off subpath imports.
 */

// --- Values ---
export {
	collection,
	single,
	BaseCollectionQuery,
	FacetCollection,
	OrderedCollectionQuery,
	OrderedFacetCollection,
} from './lib/collection.js';
export { Facet } from './lib/facet.js';
export { FacetIndex } from './lib/facet.js';
export { buildKey, Index, IndexKeyNameMap, PK, SK } from './lib/keys.js';
export {
	EmptyPatchError,
	PatchIdentityFieldError,
	PatchItemNotFoundError,
	PatchMissingKeyInputsError,
} from './lib/patch.js';
export { PartitionQuery } from './lib/query.js';
export { transactGet, transactWrite } from './lib/transact.js';

// --- Types ---
export type {
	AxisBound,
	AxisField,
	AxisValue,
	CollectionDirection,
	CollectionFailure,
	CollectionQueryOptions,
	CollectionResult,
	Grouped,
	IsSingle,
	ModelOf,
	OrderedCollectionOptions,
	OrderedCollectionQueryOptions,
	PartitionInput,
	SharedField,
	SingleMember,
	TaggedItem,
} from './lib/collection.js';

export type {
	AddIndexOptions,
	FacetConstructor,
	FacetIndexKeys,
	FacetOptions,
	FacetWithIndex,
	PickValidator,
	ReservedAttributeName,
	Validator,
	WithoutReservedAttributes,
} from './lib/facet.js';

export type {
	KeyConfiguration,
	Keys,
	PrimitiveShardKey,
	ShardConfiguration,
} from './lib/keys.js';

export type {
	DeleteFailure,
	DeleteOptions,
	DeleteResponse,
} from './lib/delete.js';

export type { GetOptions } from './lib/get.js';

export type {
	MissingPatchKeyInputs,
	PatchDemands,
	PatchFailure,
	PatchKeyInputGroups,
	PatchOf,
	PatchOptions,
	PatchSingleItemResponse,
	PatchSuccess,
} from './lib/patch.js';

export type {
	PutFailure,
	PutOptions,
	PutResponse,
	PutSingleItemResponse,
} from './lib/put.js';

export type {
	PartitionQueryOptions,
	QueryOptions,
	QueryResult,
} from './lib/query.js';

export type {
	FacetTransactionBuilders,
	TransactCheckOptions,
	TransactDeleteOptions,
	TransactFacet,
	TransactGetOp,
	TransactGetResult,
	TransactOpFailure,
	TransactPutOptions,
	TransactWriteOp,
	TransactWriteOptions,
	TransactWriteResult,
} from './lib/transact.js';

export type {
	BeginsWithCondition,
	BetweenCondition,
	Comparator,
	ComparatorCondition,
	Condition,
	ConditionExpression,
	ContainsCondition,
	ExistsCondition,
	FilterCondition,
	FilterConditionExpression,
	FilterLogicEvaluation,
	InCondition,
	LogicEvaluation,
	NotExistsCondition,
	NotExpression,
	SizeCondition,
} from './lib/expression/condition.js';

export type { AttributeMap } from './lib/converter/converter.js';
export type { ConverterOptions } from './lib/converter/converter-options.js';
