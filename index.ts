export * as Cursor from './lib/cursor';
export type {
	DeleteFailure,
	DeleteOptions,
	DeleteResponse,
} from './lib/delete';
export { Facet } from './lib/facet';
export type {
	AddIndexOptions,
	FacetIndex,
	FacetOptions,
	FacetWithIndex,
	Validator,
} from './lib/facet';
export { Index } from './lib/keys';
export type { KeyConfiguration, ShardConfiguration } from './lib/keys';
export type {
	PutFailure,
	PutOptions,
	PutResponse,
	PutSingleItemResponse,
} from './lib/put';
export type {
	PartitionQuery,
	PartitionQueryOptions,
	QueryOptions,
	QueryResult,
} from './lib/query';
