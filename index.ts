/*
 * Public API surface of `@faceteer/facet`. Anything not re-exported
 * here is internal and not covered by SemVer. The `exports` field in
 * `package.json` additionally fences off subpath imports.
 */

// --- Values ---
export { Facet } from './lib/facet.js';
export { FacetIndex } from './lib/facet.js';
export { buildKey, Index, IndexKeyNameMap, PK, SK } from './lib/keys.js';
export { PartitionQuery } from './lib/query.js';

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
