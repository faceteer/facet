/*
 * Public API surface of `@faceteer/facet`. Anything not re-exported
 * here is internal and not covered by SemVer. The `exports` field in
 * `package.json` additionally fences off subpath imports.
 */

// --- Values ---
export { Facet } from './lib/facet';
export { FacetIndex } from './lib/facet';
export {
	buildKey,
	Index,
	IndexKeyNameMap,
	PK,
	SK,
} from './lib/keys';
export { PartitionQuery } from './lib/query';

// --- Types ---
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
} from './lib/facet';

export type {
	KeyConfiguration,
	Keys,
	PrimitiveShardKey,
	ShardConfiguration,
} from './lib/keys';

export type {
	DeleteFailure,
	DeleteOptions,
	DeleteResponse,
} from './lib/delete';

export type { GetOptions } from './lib/get';

export type {
	PutFailure,
	PutOptions,
	PutResponse,
	PutSingleItemResponse,
} from './lib/put';

export type {
	PartitionQueryOptions,
	QueryOptions,
	QueryResult,
} from './lib/query';
