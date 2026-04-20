import { DynamoDB, ResourceInUseException } from '@aws-sdk/client-dynamodb';
import { Facet, PickValidator } from './facet.js';
import { Index } from './keys.js';
import * as keysModule from './keys.js';
import { wait } from './wait.js';

const ddb = new DynamoDB({
	region: 'us-east-1',
	endpoint: 'http://localhost:8000',
});

const tableName = 'TEST';

enum TokenStatus {
	Active = 'active',
	Failed = 'failed',
}

enum PostStatus {
	Draft = 'active',
	Queued = 'queued',
	Failed = 'failed',
}

enum Prefix {
	Org = '#ORG',
	Page = '#PAGE',
	Post = '#POST',
	Status = '#STATUS',
}

enum Name {
	Page = 'PAGE',
	Post = 'POST',
}

interface Page {
	pageId: string;
	pageName: string;
	accessToken: string;
	tokenStatus: TokenStatus;
	publishingDisabled?: boolean;
}
interface Post {
	pageId: string;
	postId: string;
	postStatus: PostStatus;
	postTitle?: string;
	sendAt?: Date;
	deleteAt?: number;
}

const PageFacet = new Facet({
	name: Name.Page,
	PK: {
		keys: ['pageId'],
		prefix: Prefix.Org,
	},
	SK: {
		keys: [],
		prefix: Prefix.Page,
	},
	validator: (input: unknown): Page => {
		return input as Page;
	},
	connection: {
		dynamoDb: ddb,
		tableName: tableName,
	},
});

const PostFacet = new Facet({
	name: Name.Post,
	validator: (input: unknown): Post => {
		return input as Post;
	},
	PK: {
		keys: ['pageId'],
		prefix: Prefix.Page,
	},
	SK: {
		keys: ['postId'],
		prefix: Prefix.Post,
	},
	connection: {
		dynamoDb: ddb,
		tableName: tableName,
	},
	ttl: 'deleteAt',
})
	.addIndex({
		index: Index.GSI1,
		PK: {
			keys: ['postStatus'],
			shard: { count: 4, keys: ['postId'] },
			prefix: Prefix.Status,
		},
		SK: {
			keys: ['sendAt'],
			prefix: Prefix.Status,
		},
		alias: 'GSIStatusSendAt',
	})
	.addIndex({
		index: Index.GSI2,
		PK: {
			keys: ['pageId', 'postStatus'],
			prefix: Prefix.Page,
		},
		SK: {
			keys: ['postId'],
			prefix: Prefix.Post,
		},
		alias: 'GSIPagePostStatus',
	})
	.addIndex({
		index: Index.GSI3,
		PK: {
			keys: ['pageId'],
			prefix: Prefix.Page,
		},
		SK: {
			keys: ['postTitle'],
			prefix: Prefix.Post,
		},
		alias: 'GSIPostByTitle',
	});

const mockPageIds: string[] = [];

describe('Facet', () => {
	beforeAll(async () => {
		await createTestTable();
		const pages = mockPages(5);
		const pagePutResult = await PageFacet.put(pages);
		if (pagePutResult.hasFailures) {
			throw new Error('Unable to put mock pages for test');
		}
		const posts: Post[] = [];

		for (const [index, page] of pages.entries()) {
			mockPageIds.push(page.pageId);
			const draftPosts = mockPosts(100, {
				pageId: page.pageId,
				postStatus: PostStatus.Draft,
			});

			const queuedPosts = mockPosts(100, {
				pageId: page.pageId,
				postStatus: PostStatus.Queued,
				sendAt: new Date(`2021-0${index + 1}-10T03:38:53.601Z`),
			});

			const failed = mockPosts(100, {
				pageId: page.pageId,
				postStatus: PostStatus.Failed,
				sendAt: new Date(`2021-0${index + 1}-10T03:38:53.601Z`),
			});
			posts.push(...draftPosts, ...queuedPosts, ...failed);
		}

		const postPutResult = await PostFacet.put(posts);
		if (postPutResult.hasFailures) {
			throw new Error('Unable to put mock posts for test');
		}
	});

	test('Get Pages', async () => {
		const firstPage = await PageFacet.get({
			pageId: mockPageIds[0],
		});

		expect(firstPage?.pageId).toBe(mockPageIds[0]);
		expect(firstPage).not.toHaveProperty('facet');
		expect(firstPage).not.toHaveProperty('PK');
		expect(firstPage).not.toHaveProperty('SK');

		const allPages = await PageFacet.get(
			mockPageIds.map((pageId) => ({ pageId })),
		);

		expect(allPages.length).toBe(5);
	});

	test('List Posts', async () => {
		const posts = await PostFacet.query({ pageId: mockPageIds[0] }).list();

		expect(posts.records.length).toBe(300);

		const queuedPosts = await PostFacet.GSIPagePostStatus.query({
			pageId: mockPageIds[0],
			postStatus: PostStatus.Queued,
		}).list();

		expect(queuedPosts.records.length).toBe(100);

		const noFailed = await PostFacet.query({ pageId: mockPageIds[1] }).list({
			filter: ['postStatus', '<>', PostStatus.Failed],
		});

		expect(noFailed.records.length).toBe(200);
	});

	test('Paginate', async () => {
		const pagePostPartition = PostFacet.query({ pageId: mockPageIds[0] });
		const firstPage = await pagePostPartition.list({
			limit: 10,
		});

		expect(firstPage.records.length).toBe(10);
		expect(firstPage.cursor).toBeDefined();

		const nextPage = await pagePostPartition.list({
			cursor: firstPage.cursor,
			limit: 10,
		});

		expect(nextPage.records.length).toBe(10);
	});

	test('Delete Pages', async () => {
		const [pageToDelete] = mockPages(1);
		const putPageResult = await PageFacet.put(pageToDelete);

		expect(putPageResult.wasSuccessful).toBeTruthy();

		if (!putPageResult.wasSuccessful) {
			throw Error('Unable to put the page to delete');
		}

		const deleteResult = await PageFacet.delete({
			pageId: putPageResult.record.pageId,
		});

		expect(deleteResult.deleted.length).toBe(1);
	});

	test('Delete Multiple Pages', async () => {
		const pagesToDelete = mockPages(101);

		const { put: putPages } = await PageFacet.put(pagesToDelete);

		expect(putPages.length).toEqual(101);

		const pageIdsToDelete = pagesToDelete.map((page) => ({
			pageId: page.pageId,
		}));

		const deleteResult = await PageFacet.delete(pageIdsToDelete);

		expect(deleteResult.deleted.length).toEqual(101);

		const expectedMissingPages = await PageFacet.get(pageIdsToDelete);

		expect(expectedMissingPages.length).toEqual(0);
	});

	test('Test Conditional Queries', async () => {
		const [page] = mockPages(1);
		const posts = [
			...mockPosts(10, {
				pageId: page.pageId,
				postStatus: PostStatus.Draft,
				postTitle: 'aa',
			}),
			...mockPosts(10, {
				pageId: page.pageId,
				postStatus: PostStatus.Failed,
				postTitle: 'ad',
			}),
			...mockPosts(10, {
				pageId: page.pageId,
				postStatus: PostStatus.Queued,
				postTitle: 'bb',
			}),
		];

		await PageFacet.put(page);

		await PostFacet.put(posts);

		// `ad` pages
		const middlePosts = await PostFacet.GSIPostByTitle.query(page).between(
			{ postTitle: 'ab' },
			{ postTitle: 'ae' },
		);
		expect(middlePosts.records.length).toBe(10);

		// `ad` and `aa` pages.
		const firstPostsInclusive = await PostFacet.GSIPostByTitle.query(
			page,
		).lessThanOrEqual({ postTitle: 'ad' });
		expect(firstPostsInclusive.records.length).toBe(20);

		// `aa` pages
		const firstPostsExclusive = await PostFacet.GSIPostByTitle.query(
			page,
		).lessThan({ postTitle: 'ad' });
		expect(firstPostsExclusive.records.length).toBe(10);
		for (const post of firstPostsExclusive.records) {
			expect(post.postTitle).toEqual('aa');
		}

		// `ad` and `bb` pages.
		const lastPostsInclusive = await PostFacet.GSIPostByTitle.query(
			page,
		).greaterThanOrEqual({ postTitle: 'ad' });
		expect(lastPostsInclusive.records.length).toBe(20);

		//  `bb` pages
		const lastPostsExclusive = await PostFacet.GSIPostByTitle.query(
			page,
		).greaterThan({ postTitle: 'ad' });
		expect(lastPostsExclusive.records.length).toBe(10);
		for (const post of lastPostsExclusive.records) {
			expect(post.postTitle).toEqual('bb');
		}

		// Posts that start with `a`
		const aPosts = await PostFacet.GSIPostByTitle.query(page).beginsWith({
			postTitle: 'a',
		});
		expect(aPosts.records.length).toBe(20);
	});

	test('ShardConfiguration.keys rejects non-primitive fields', () => {
		// Issue #44: shard.keys was typed `Keys<T>[]`, so a Date/object/array
		// field could be named as a shard key and the hash input would
		// silently become `[object Object]` / a timezone-dependent string.
		// The fix narrows shard.keys to `PrimitiveShardKey<T>`.
		interface Model {
			id: string;
			count: number;
			flag: boolean;
			big: bigint;
			optional?: string;
			sentAt: Date;
			nested: { inner: string };
			list: string[];
			// Locks in that PrimitiveShardKey non-distributes over unions: a
			// union that mixes a primitive and a non-primitive must be
			// rejected as a whole, not accepted for the primitive half.
			mixed: string | Date;
		}

		void (async () => {
			// Primitives and optionals are accepted.
			new Facet<Model, 'id'>({
				name: 'M',
				validator: (i) => i as Model,
				PK: {
					keys: ['id'],
					prefix: 'M',
					shard: { count: 4, keys: ['id', 'count', 'flag', 'big', 'optional'] },
				},
				SK: { keys: [], prefix: 'M' },
				connection: { dynamoDb: ddb, tableName },
			});

			// Non-primitive shard keys are a type error.
			new Facet<Model, 'id'>({
				name: 'M',
				validator: (i) => i as Model,
				PK: {
					keys: ['id'],
					prefix: 'M',
					// @ts-expect-error Date is not a primitive shard-key type
					shard: { count: 4, keys: ['sentAt'] },
				},
				SK: { keys: [], prefix: 'M' },
				connection: { dynamoDb: ddb, tableName },
			});
			new Facet<Model, 'id'>({
				name: 'M',
				validator: (i) => i as Model,
				PK: {
					keys: ['id'],
					prefix: 'M',
					// @ts-expect-error object is not a primitive shard-key type
					shard: { count: 4, keys: ['nested'] },
				},
				SK: { keys: [], prefix: 'M' },
				connection: { dynamoDb: ddb, tableName },
			});
			new Facet<Model, 'id'>({
				name: 'M',
				validator: (i) => i as Model,
				PK: {
					keys: ['id'],
					prefix: 'M',
					// @ts-expect-error array is not a primitive shard-key type
					shard: { count: 4, keys: ['list'] },
				},
				SK: { keys: [], prefix: 'M' },
				connection: { dynamoDb: ddb, tableName },
			});
			new Facet<Model, 'id'>({
				name: 'M',
				validator: (i) => i as Model,
				PK: {
					keys: ['id'],
					prefix: 'M',
					// @ts-expect-error a field typed `string | Date` is rejected as a whole
					shard: { count: 4, keys: ['mixed'] },
				},
				SK: { keys: [], prefix: 'M' },
				connection: { dynamoDb: ddb, tableName },
			});
		});

		expect<0>(0 satisfies 0).toBe(0);
	});

	test('dead-code exports are removed from lib/keys', () => {
		// Issue #42: IndexPrivatePropertyMap, isIndex, IndexKeyConfiguration,
		// and IndexKeyOptions were declared but never used. Guard against
		// them being re-added.
		// @ts-expect-error IndexPrivatePropertyMap was removed as dead code
		void keysModule.IndexPrivatePropertyMap;
		// @ts-expect-error isIndex was removed as dead code
		void keysModule.isIndex;
		// @ts-expect-error IndexKeyConfiguration was removed as dead code
		type _IKC = keysModule.IndexKeyConfiguration<unknown, never, never>;
		// @ts-expect-error IndexKeyOptions was removed as dead code
		type _IKO = keysModule.IndexKeyOptions<unknown>;

		expect<0>(0 satisfies 0).toBe(0);
	});

	test('base-facet query sort-key argument is typed against the facet SK', () => {
		// Issue #41: before the fix, every query sort-key arg was typed
		// `Partial<Pick<T, GSISK>>`, which collapses to `{}` on base-facet
		// queries (where GSISK is never). That meant the sort arg accepted
		// any object and no field was checked. The fix threads the active
		// SK (base SK or index GSISK) through the method signatures.
		void (async () => {
			// Base query: SK is postId. Typechecks.
			await PostFacet.query({ pageId: 'p' }).equals({ postId: 'x' });
			await PostFacet.query({ pageId: 'p' }).greaterThan({ postId: 'x' });
			await PostFacet.query({ pageId: 'p' }).greaterThanOrEqual({
				postId: 'x',
			});
			await PostFacet.query({ pageId: 'p' }).lessThan({ postId: 'x' });
			await PostFacet.query({ pageId: 'p' }).lessThanOrEqual({ postId: 'x' });
			await PostFacet.query({ pageId: 'p' }).beginsWith({ postId: 'x' });
			await PostFacet.query({ pageId: 'p' }).between(
				{ postId: 'a' },
				{ postId: 'z' },
			);

			// Non-SK fields on a base query are a type error.
			// @ts-expect-error postStatus is not a base-facet SK field
			await PostFacet.query({ pageId: 'p' }).equals({ postStatus: 'x' });
			// @ts-expect-error sendAt is not a base-facet SK field
			await PostFacet.query({ pageId: 'p' }).beginsWith({ sendAt: new Date() });
			await PostFacet.query({ pageId: 'p' }).between(
				// @ts-expect-error sendAt is not a base-facet SK field on between's start arg
				{ sendAt: new Date() },
				'raw',
			);
			await PostFacet.query({ pageId: 'p' }).between(
				'raw',
				// @ts-expect-error sendAt is not a base-facet SK field on between's end arg
				{ sendAt: new Date() },
			);

			// Index query: SK is postTitle. Non-GSISK field is a type error.
			await PostFacet.GSIPostByTitle.query({ pageId: 'p' }).equals({
				postTitle: 'x',
			});
			await PostFacet.GSIPostByTitle.query({ pageId: 'p' })
				// @ts-expect-error postId is not an index SK field
				.equals({ postId: 'x' });

			// The string escape hatch keeps working on both.
			await PostFacet.query({ pageId: 'p' }).equals('raw');
			await PostFacet.GSIPostByTitle.query({ pageId: 'p' }).equals('raw');
		});

		expect<0>(0 satisfies 0).toBe(0);
	});

	test('reserved attribute names fail the type-level constraint', () => {
		// Type-only assertions — `npm run typecheck` (which covers tests via
		// the base tsconfig.json) is the real check. At runtime this is a no-op.
		// @ts-expect-error — PK is a reserved attribute name
		type _PkModel = Facet<{ PK: string; id: string }>;
		// @ts-expect-error — ttl is a reserved attribute name
		type _TtlModel = Facet<{ ttl: number; id: string }>;
		// @ts-expect-error — GSI1PK matches the reserved GSI{number}PK pattern
		type _GsiModel = Facet<{ GSI1PK: string; id: string }>;

		// Silence the unused-locals rule without emitting runtime code.
		expect<0>(0 satisfies 0).toBe(0);
	});

	test('in() throws when the model contains a reserved attribute', async () => {
		interface Thing {
			thingId: string;
			payload: string;
		}
		const ThingFacet = new Facet<Thing, 'thingId'>({
			name: 'Thing',
			validator: (input) => input as Thing,
			PK: { keys: ['thingId'], prefix: 'T' },
			SK: { keys: [], prefix: 'T' },
			connection: { dynamoDb: ddb, tableName },
		});

		// Bypass the type-level guard the way a dynamic caller would —
		// the runtime assertion is a backstop for that case.
		const withPk = { thingId: 'x', payload: 'ok', PK: 'stolen' };
		const withTtl = { thingId: 'x', payload: 'ok', ttl: 42 };
		const withFacet = { thingId: 'x', payload: 'ok', facet: 'hijack' };
		const withGsi = { thingId: 'x', payload: 'ok', GSI1PK: 'stolen' };

		expect(() => ThingFacet.in(withPk as unknown as Thing)).toThrow(
			/reserved attribute "PK"/,
		);
		expect(() => ThingFacet.in(withTtl as unknown as Thing)).toThrow(
			/reserved attribute "ttl"/,
		);
		expect(() => ThingFacet.in(withFacet as unknown as Thing)).toThrow(
			/reserved attribute "facet"/,
		);
		expect(() => ThingFacet.in(withGsi as unknown as Thing)).toThrow(
			/reserved attribute "GSI1PK"/,
		);
	});

	test('addIndex rejects re-registering a GSI slot', async () => {
		interface Thing {
			thingId: string;
			status: string;
		}
		const base = new Facet<Thing, 'thingId'>({
			name: 'Thing',
			validator: (input) => input as Thing,
			PK: { keys: ['thingId'], prefix: 'T' },
			SK: { keys: [], prefix: 'T' },
			connection: { dynamoDb: ddb, tableName },
		}).addIndex({
			index: Index.GSI1,
			PK: { keys: ['status'], prefix: 'S' },
			SK: { keys: ['thingId'], prefix: 'T' },
			alias: 'byStatus',
		});

		expect(() =>
			base.addIndex({
				index: Index.GSI1,
				PK: { keys: ['status'], prefix: 'S' },
				SK: { keys: ['thingId'], prefix: 'T' },
				alias: 'byStatusAgain',
			}),
		).toThrow(/already registered/);
	});

	test('SK.shard config produces sharded sort keys', async () => {
		interface Event {
			eventType: string;
			eventId: string;
		}
		const EventFacet = new Facet<Event, 'eventType', 'eventId'>({
			name: 'Event',
			validator: (input) => input as Event,
			PK: { keys: ['eventType'], prefix: 'EVENT' },
			SK: {
				keys: ['eventId'],
				prefix: 'ID',
				shard: { count: 4, keys: ['eventId'] },
			},
			connection: { dynamoDb: ddb, tableName },
		});

		const model = { eventType: 'click', eventId: 'some-event' };

		const explicit0 = EventFacet.sk(model, 0);
		const explicit1 = EventFacet.sk(model, 1);
		const hashed = EventFacet.sk(model);

		// count = 4 → padLength = 1 → single hex digit in the shard slot.
		expect(explicit0).toBe(`ID_0_${model.eventId}`);
		expect(explicit1).toBe(`ID_1_${model.eventId}`);
		expect(hashed).toMatch(/^ID_[0-3]_some-event$/);
	});

	test('multi-key shard inputs are delimiter-separated before hashing', () => {
		// Without a separator, `{a: 'ab', b: 'c'}` and `{a: 'a', b: 'bc'}`
		// would stringify-concat to the same 'abc' and share a shard. The
		// null-byte separator makes them distinct. Using a large shard
		// count makes a collision vanishingly unlikely under CRC32.
		interface Thing {
			kind: string;
			a: string;
			b: string;
		}
		const ThingFacet = new Facet<Thing, 'kind'>({
			name: 'Thing',
			validator: (input) => input as Thing,
			PK: {
				keys: ['kind'],
				prefix: 'T',
				shard: { count: 256, keys: ['a', 'b'] },
			},
			SK: { keys: [], prefix: 'T' },
			connection: { dynamoDb: ddb, tableName },
		});

		const left = ThingFacet.pk({ kind: 'k', a: 'ab', b: 'c' });
		const right = ThingFacet.pk({ kind: 'k', a: 'a', b: 'bc' });
		expect(left).not.toEqual(right);
	});

	test('Sharded PK honours explicit shard: 0', async () => {
		// GSI1 is sharded with count 4 on `postId`.
		const model = {
			postId: 'some-post-id',
			postStatus: PostStatus.Queued,
		};
		const shard0 = PostFacet.GSIStatusSendAt.pk(model, 0);
		const shard1 = PostFacet.GSIStatusSendAt.pk(model, 1);
		const shard2 = PostFacet.GSIStatusSendAt.pk(model, 2);
		const shard3 = PostFacet.GSIStatusSendAt.pk(model, 3);
		const hashed = PostFacet.GSIStatusSendAt.pk(model);

		// count = 4, so padLength = 1 and shard IDs are single hex digits.
		expect(shard0).toBe(`${Prefix.Status}_0_${PostStatus.Queued}`);
		expect(shard1).toBe(`${Prefix.Status}_1_${PostStatus.Queued}`);
		expect(shard2).toBe(`${Prefix.Status}_2_${PostStatus.Queued}`);
		expect(shard3).toBe(`${Prefix.Status}_3_${PostStatus.Queued}`);

		// The hashed key for this model happens not to be shard 0, but
		// the fundamental invariant is that explicit shard 0 differs
		// from at least one of the other explicit shards — otherwise
		// it was silently treated as "unspecified".
		expect(new Set([shard0, shard1, shard2, shard3]).size).toBe(4);
		expect([shard0, shard1, shard2, shard3]).toContain(hashed);
	});

	test('Conditional Deletes', async () => {
		const [page] = mockPages(1);
		const putResult = await PageFacet.put(page);
		expect(putResult.wasSuccessful).toBe(true);

		const successfulDelete = await PageFacet.delete(
			{ pageId: page.pageId },
			{ condition: ['pageId', 'exists'] },
		);

		expect(successfulDelete.hasFailures).toBe(false);
		expect(successfulDelete.deleted).toHaveLength(1);

		const failedDelete = await PageFacet.delete(
			{ pageId: page.pageId },
			{ condition: ['pageId', 'exists'] },
		);

		expect(failedDelete.hasFailures).toBe(true);
		expect(failedDelete.deleted).toHaveLength(0);
	});

	test('Conditional Puts', async () => {
		const testPage: Page = {
			accessToken: 'ZZZZZZZZZZZZ',
			pageId: 'ZZZZZZZZZZZZ',
			pageName: 'ZZZZZZZZZZZZ',
			tokenStatus: TokenStatus.Failed,
		};

		await PageFacet.delete({ pageId: testPage.pageId });
		const successfulPut = await PageFacet.put(testPage, {
			condition: ['pageId', 'not_exists'],
		});

		expect(successfulPut.wasSuccessful).toBeTruthy();

		const failedPut = await PageFacet.put(testPage, {
			condition: ['pageId', 'not_exists'],
		});

		expect(failedPut.wasSuccessful).toBeFalsy();
	});

	test('TTL Date fields are written as epoch-seconds numbers', async () => {
		interface Session {
			sessionId: string;
			expiresAt: Date;
		}
		const SessionFacet = new Facet<Session, 'sessionId'>({
			name: 'Session',
			validator: (input) => input as Session,
			PK: { keys: ['sessionId'], prefix: 'SESSION' },
			SK: { keys: [], prefix: 'SESSION' },
			connection: { dynamoDb: ddb, tableName },
			ttl: 'expiresAt',
		});

		const expiresAt = new Date('2030-01-02T03:04:05.000Z');
		const sessionId = 'ttl-session-1';
		const expectedSeconds = Math.floor(expiresAt.getTime() / 1000);

		const putResult = await SessionFacet.put({ sessionId, expiresAt });
		expect(putResult.wasSuccessful).toBe(true);

		const raw = await ddb.getItem({
			TableName: tableName,
			Key: {
				PK: { S: SessionFacet.pk({ sessionId }) },
				SK: { S: SessionFacet.sk({ sessionId }) },
			},
		});

		expect(raw.Item?.ttl).toEqual({ N: String(expectedSeconds) });
	});

	test('out() strips ttl even when facet has no indexes', async () => {
		interface Session {
			sessionId: string;
			expiresAt: Date;
		}
		const seen: string[][] = [];
		const SessionFacet = new Facet<Session, 'sessionId'>({
			name: 'SessionStrict',
			validator: (input) => {
				const keys = Object.keys(input as object).sort();
				seen.push(keys);
				if (keys.includes('ttl')) {
					throw new Error(`Unexpected ttl attribute: ${keys.join(',')}`);
				}
				return input as Session;
			},
			PK: { keys: ['sessionId'], prefix: 'SESSIONX' },
			SK: { keys: [], prefix: 'SESSIONX' },
			connection: { dynamoDb: ddb, tableName },
			ttl: 'expiresAt',
		});

		const sessionId = 'ttl-session-strict';
		const expiresAt = new Date('2030-01-02T03:04:05.000Z');
		const putResult = await SessionFacet.put({ sessionId, expiresAt });
		expect(putResult.wasSuccessful).toBe(true);

		const fetched = await SessionFacet.get({ sessionId });
		expect(fetched?.sessionId).toBe(sessionId);
		expect(seen.at(-1)).toEqual(['expiresAt', 'sessionId']);
	});

	test('TTL string fields are parsed to epoch-seconds numbers', async () => {
		interface Token {
			tokenId: string;
			deleteAt: string;
		}
		const TokenFacet = new Facet<Token, 'tokenId'>({
			name: 'Token',
			validator: (input) => input as Token,
			PK: { keys: ['tokenId'], prefix: 'TOKEN' },
			SK: { keys: [], prefix: 'TOKEN' },
			connection: { dynamoDb: ddb, tableName },
			ttl: 'deleteAt',
		});

		const tokenId = 'ttl-token-1';
		const deleteAt = '1893459845';

		const putResult = await TokenFacet.put({ tokenId, deleteAt });
		expect(putResult.wasSuccessful).toBe(true);

		const raw = await ddb.getItem({
			TableName: tableName,
			Key: {
				PK: { S: TokenFacet.pk({ tokenId }) },
				SK: { S: TokenFacet.sk({ tokenId }) },
			},
		});

		expect(raw.Item?.ttl).toEqual({ N: deleteAt });
	});

	describe('Projection (select)', () => {
		interface Product {
			productId: string;
			sku: string;
			name: string;
			price: number;
			description?: string;
		}

		const productValidator = (input: unknown): Product => {
			const record = input as Record<string, unknown>;
			if (
				typeof record.productId !== 'string' ||
				typeof record.sku !== 'string' ||
				typeof record.name !== 'string' ||
				typeof record.price !== 'number'
			) {
				throw new Error('invalid product');
			}
			return record as unknown as Product;
		};

		const productPickValidator: PickValidator<Product> = (keys) => (input) => {
			const record = input as Record<string, unknown>;
			const picked: Record<string, unknown> = {};
			for (const key of keys) {
				if (key in record) {
					picked[key as string] = record[key as string];
				}
			}
			return picked as Pick<Product, (typeof keys)[number]>;
		};

		const ProjectableFacet = new Facet<Product, 'productId', 'sku'>({
			name: 'Product',
			validator: productValidator,
			pickValidator: productPickValidator,
			PK: { keys: ['productId'], prefix: 'PROD' },
			SK: { keys: ['sku'], prefix: 'SKU' },
			connection: { dynamoDb: ddb, tableName },
		});

		const PlainFacet = new Facet<Product, 'productId', 'sku'>({
			name: 'ProductPlain',
			validator: productValidator,
			PK: { keys: ['productId'], prefix: 'PLAIN' },
			SK: { keys: ['sku'], prefix: 'PLAIN' },
			connection: { dynamoDb: ddb, tableName },
		});

		test('single get with select returns only chosen + key fields', async () => {
			await ProjectableFacet.put({
				productId: 'p-1',
				sku: 's-1',
				name: 'Widget',
				price: 9.99,
				description: 'A useful widget',
			});

			const projected = await ProjectableFacet.get(
				{ productId: 'p-1', sku: 's-1' },
				{ select: ['name'] },
			);

			expect(projected).not.toBeNull();
			expect(projected?.productId).toBe('p-1');
			expect(projected?.sku).toBe('s-1');
			expect(projected?.name).toBe('Widget');
			expect(projected).not.toHaveProperty('price');
			expect(projected).not.toHaveProperty('description');
			expect(projected).not.toHaveProperty('PK');
			expect(projected).not.toHaveProperty('SK');
			expect(projected).not.toHaveProperty('facet');
		});

		test('batch get with select returns projected records', async () => {
			const products: Product[] = [
				{ productId: 'p-2', sku: 's-2', name: 'A', price: 1 },
				{ productId: 'p-3', sku: 's-3', name: 'B', price: 2 },
			];
			await ProjectableFacet.put(products);

			const projected = await ProjectableFacet.get(
				products.map((p) => ({ productId: p.productId, sku: p.sku })),
				{ select: ['price'] },
			);

			expect(projected).toHaveLength(2);
			for (const record of projected) {
				expect(record).toHaveProperty('productId');
				expect(record).toHaveProperty('sku');
				expect(record).toHaveProperty('price');
				expect(record).not.toHaveProperty('name');
				expect(record).not.toHaveProperty('description');
			}
		});

		test('select runs pickValidator, not the full validator', async () => {
			let fullValidatorCalls = 0;
			let pickValidatorCalls = 0;

			const countingPickValidator: PickValidator<Product> =
				(keys) => (input) => {
					pickValidatorCalls += 1;
					return productPickValidator(keys)(input);
				};

			const CountingFacet = new Facet<Product, 'productId', 'sku'>({
				name: 'ProductPickCount',
				validator: (input) => {
					fullValidatorCalls += 1;
					return productValidator(input);
				},
				pickValidator: countingPickValidator,
				PK: { keys: ['productId'], prefix: 'PICKCNT' },
				SK: { keys: ['sku'], prefix: 'PICKCNT' },
				connection: { dynamoDb: ddb, tableName },
			});

			await CountingFacet.put({
				productId: 'pc-1',
				sku: 'pc-1',
				name: 'X',
				price: 0,
			});

			fullValidatorCalls = 0;
			pickValidatorCalls = 0;

			await CountingFacet.get(
				{ productId: 'pc-1', sku: 'pc-1' },
				{ select: ['name'] },
			);

			expect(pickValidatorCalls).toBe(1);
			expect(fullValidatorCalls).toBe(0);
		});

		test('select narrows the return type', async () => {
			await ProjectableFacet.put({
				productId: 'type-1',
				sku: 'type-1',
				name: 'X',
				price: 0,
			});

			const projected = await ProjectableFacet.get(
				{ productId: 'type-1', sku: 'type-1' },
				{ select: ['name'] },
			);
			if (!projected) throw new Error('expected record');

			const _name: string = projected.name;
			const _productId: string = projected.productId;
			const _sku: string = projected.sku;
			void _name;
			void _productId;
			void _sku;

			// @ts-expect-error `price` was not selected, so it is not in the type
			void projected.price;
			// @ts-expect-error `description` was not selected
			void projected.description;
		});

		test('get without select still works on a projectable facet', async () => {
			await ProjectableFacet.put({
				productId: 'plain-get-1',
				sku: 'plain-get-1',
				name: 'Y',
				price: 1,
			});

			const full = await ProjectableFacet.get({
				productId: 'plain-get-1',
				sku: 'plain-get-1',
			});
			expect(full?.name).toBe('Y');
			expect(full?.price).toBe(1);
		});

		test('get without select still works on a plain facet', async () => {
			await PlainFacet.put({
				productId: 'plain-1',
				sku: 'plain-1',
				name: 'Z',
				price: 2,
			});
			const full = await PlainFacet.get({
				productId: 'plain-1',
				sku: 'plain-1',
			});
			expect(full?.name).toBe('Z');
		});

		test('type gate: select is unavailable on facets without pickValidator', () => {
			void (async () => {
				// @ts-expect-error select is gated at the type level behind pickValidator
				await PlainFacet.get(
					{ productId: 'x', sku: 'x' },
					{ select: ['name'] },
				);
				// @ts-expect-error batch form is gated the same way
				await PlainFacet.get([{ productId: 'x', sku: 'x' }], {
					select: ['name'],
				});
			});
		});

		test('select with duplicate keys is deduped', async () => {
			await ProjectableFacet.put({
				productId: 'dup-1',
				sku: 'dup-1',
				name: 'D',
				price: 3,
			});
			const projected = await ProjectableFacet.get(
				{ productId: 'dup-1', sku: 'dup-1' },
				{ select: ['name', 'name', 'price', 'name'] },
			);
			expect(projected?.name).toBe('D');
			expect(projected?.price).toBe(3);
		});

		test('select that includes a PK/SK field does not double-project', async () => {
			await ProjectableFacet.put({
				productId: 'overlap-1',
				sku: 'overlap-1',
				name: 'O',
				price: 4,
			});
			// productId is the PK and is auto-included; listing it explicitly
			// in `select` must not produce a DDB error (duplicate placeholder)
			// or drop other selected fields.
			const projected = await ProjectableFacet.get(
				{ productId: 'overlap-1', sku: 'overlap-1' },
				{ select: ['productId', 'name'] },
			);
			expect(projected?.productId).toBe('overlap-1');
			expect(projected?.sku).toBe('overlap-1');
			expect(projected?.name).toBe('O');
			expect(projected).not.toHaveProperty('price');
		});
	});

	describe('Query projection (select)', () => {
		interface Row {
			userId: string;
			rowId: string;
			status: string;
			createdAt: string;
			name: string;
			body: string;
			count: number;
		}

		const rowValidator = (input: unknown): Row => input as Row;
		const rowPickValidator: PickValidator<Row> = (keys) => (input) => {
			const record = input as Record<string, unknown>;
			const picked: Record<string, unknown> = {};
			for (const key of keys) {
				if (key in record) {
					picked[key as string] = record[key as string];
				}
			}
			return picked as Pick<Row, (typeof keys)[number]>;
		};

		const ProjectableQueryFacet = new Facet<Row, 'userId', 'rowId'>({
			name: 'QueryRow',
			validator: rowValidator,
			pickValidator: rowPickValidator,
			PK: { keys: ['userId'], prefix: 'QUSER' },
			SK: { keys: ['rowId'], prefix: 'QROW' },
			connection: { dynamoDb: ddb, tableName },
		}).addIndex({
			alias: 'byStatus',
			index: Index.GSI1,
			PK: { keys: ['userId', 'status'], prefix: 'QSTATUS' },
			SK: { keys: ['createdAt'], prefix: 'QCREATED' },
		});

		const PlainQueryFacet = new Facet<Row, 'userId', 'rowId'>({
			name: 'QueryRowPlain',
			validator: rowValidator,
			PK: { keys: ['userId'], prefix: 'QPLAIN' },
			SK: { keys: ['rowId'], prefix: 'QPLAIN' },
			connection: { dynamoDb: ddb, tableName },
		});

		const USER = 'query-user-1';
		const rows: Row[] = [
			{
				userId: USER,
				rowId: 'r-001',
				status: 'queued',
				createdAt: '2024-01-01T00:00:00.000Z',
				name: 'Alpha',
				body: 'a body',
				count: 1,
			},
			{
				userId: USER,
				rowId: 'r-002',
				status: 'queued',
				createdAt: '2024-02-01T00:00:00.000Z',
				name: 'Bravo',
				body: 'b body',
				count: 2,
			},
			{
				userId: USER,
				rowId: 'r-003',
				status: 'sent',
				createdAt: '2024-03-01T00:00:00.000Z',
				name: 'Charlie',
				body: 'c body',
				count: 3,
			},
		];

		beforeAll(async () => {
			await ProjectableQueryFacet.put(rows);
		});

		test('list with select returns only chosen + key fields', async () => {
			const result = await ProjectableQueryFacet.query({
				userId: USER,
			}).list({ select: ['name'] });

			expect(result.records).toHaveLength(3);
			for (const record of result.records) {
				expect(record).toHaveProperty('userId');
				expect(record).toHaveProperty('rowId');
				expect(record).toHaveProperty('name');
				expect(record).not.toHaveProperty('body');
				expect(record).not.toHaveProperty('count');
				expect(record).not.toHaveProperty('status');
			}
		});

		test('first with select returns a projected record', async () => {
			const record = await ProjectableQueryFacet.query({
				userId: USER,
			}).first({ select: ['body'] });

			expect(record).not.toBeNull();
			expect(record?.userId).toBe(USER);
			expect(record?.rowId).toBe('r-001');
			expect(record?.body).toBe('a body');
			expect(record).not.toHaveProperty('name');
		});

		test('equals with select narrows to one row', async () => {
			const result = await ProjectableQueryFacet.query({
				userId: USER,
			}).equals({ rowId: 'r-002' }, { select: ['count'] });

			expect(result.records).toHaveLength(1);
			expect(result.records[0].rowId).toBe('r-002');
			expect(result.records[0].count).toBe(2);
			expect(result.records[0]).not.toHaveProperty('name');
		});

		test('greaterThan with select', async () => {
			const result = await ProjectableQueryFacet.query({
				userId: USER,
			}).greaterThan({ rowId: 'r-001' }, { select: ['body'] });

			expect(result.records.map((r) => [r.rowId, r.body])).toEqual([
				['r-002', 'b body'],
				['r-003', 'c body'],
			]);
			for (const record of result.records) {
				expect(record).not.toHaveProperty('name');
			}
		});

		test('greaterThanOrEqual with select', async () => {
			const result = await ProjectableQueryFacet.query({
				userId: USER,
			}).greaterThanOrEqual({ rowId: 'r-002' }, { select: ['name'] });

			expect(result.records.map((r) => [r.rowId, r.name])).toEqual([
				['r-002', 'Bravo'],
				['r-003', 'Charlie'],
			]);
			for (const record of result.records) {
				expect(record).not.toHaveProperty('body');
			}
		});

		test('lessThan with select', async () => {
			const result = await ProjectableQueryFacet.query({
				userId: USER,
			}).lessThan({ rowId: 'r-003' }, { select: ['count'] });

			expect(result.records.map((r) => [r.rowId, r.count])).toEqual([
				['r-001', 1],
				['r-002', 2],
			]);
			for (const record of result.records) {
				expect(record).not.toHaveProperty('body');
			}
		});

		test('lessThanOrEqual with select', async () => {
			const result = await ProjectableQueryFacet.query({
				userId: USER,
			}).lessThanOrEqual({ rowId: 'r-002' }, { select: ['name'] });

			expect(result.records.map((r) => [r.rowId, r.name])).toEqual([
				['r-001', 'Alpha'],
				['r-002', 'Bravo'],
			]);
			for (const record of result.records) {
				expect(record).not.toHaveProperty('body');
			}
		});

		test('beginsWith with select', async () => {
			const result = await ProjectableQueryFacet.query({
				userId: USER,
			}).beginsWith({ rowId: 'r-' }, { select: ['name'] });

			expect(result.records).toHaveLength(3);
			for (const record of result.records) {
				expect(record).toHaveProperty('userId');
				expect(record).toHaveProperty('rowId');
				expect(record).toHaveProperty('name');
				expect(record).not.toHaveProperty('body');
			}
		});

		test('between with select', async () => {
			const result = await ProjectableQueryFacet.query({
				userId: USER,
			}).between({ rowId: 'r-001' }, { rowId: 'r-002' }, { select: ['body'] });

			expect(result.records.map((r) => [r.rowId, r.body])).toEqual([
				['r-001', 'a body'],
				['r-002', 'b body'],
			]);
			for (const record of result.records) {
				expect(record).not.toHaveProperty('count');
			}
		});

		test('index query with select auto-includes GSI PK/SK fields', async () => {
			const result = await ProjectableQueryFacet.byStatus
				.query({ userId: USER, status: 'queued' })
				.list({ select: ['name'] });

			expect(result.records).toHaveLength(2);
			for (const record of result.records) {
				// base PK + base SK (ALL projection on the GSI)
				expect(record).toHaveProperty('userId');
				expect(record).toHaveProperty('rowId');
				// index PK field (status) and index SK field (createdAt)
				expect(record).toHaveProperty('status');
				expect(record).toHaveProperty('createdAt');
				// selected
				expect(record).toHaveProperty('name');
				// not selected
				expect(record).not.toHaveProperty('body');
			}
		});

		test('query projection select narrows the return type', async () => {
			const result = await ProjectableQueryFacet.query({
				userId: USER,
			}).list({ select: ['name'] });

			if (result.records.length === 0) throw new Error('expected rows');
			const row = result.records[0];

			const _userId: string = row.userId;
			const _rowId: string = row.rowId;
			const _name: string = row.name;
			void _userId;
			void _rowId;
			void _name;

			// @ts-expect-error body was not selected on a base-facet query
			void row.body;
			// @ts-expect-error count was not selected
			void row.count;
		});

		test('index projection narrows return type and auto-includes index keys', async () => {
			const result = await ProjectableQueryFacet.byStatus
				.query({ userId: USER, status: 'queued' })
				.list({ select: ['name'] });

			if (result.records.length === 0) throw new Error('expected rows');
			const row = result.records[0];

			const _name: string = row.name;
			const _userId: string = row.userId;
			const _rowId: string = row.rowId;
			const _status: string = row.status;
			const _createdAt: string = row.createdAt;
			void _name;
			void _userId;
			void _rowId;
			void _status;
			void _createdAt;

			// @ts-expect-error body was not selected
			void row.body;
		});

		test('select with duplicate keys is deduped', async () => {
			const result = await ProjectableQueryFacet.query({
				userId: USER,
			}).list({ select: ['name', 'name', 'body', 'name'] });

			expect(result.records).toHaveLength(3);
			for (const record of result.records) {
				expect(record).toHaveProperty('name');
				expect(record).toHaveProperty('body');
				expect(record).not.toHaveProperty('count');
			}
		});

		test('select that includes an auto-included key field does not double-project', async () => {
			// userId is the PK and is auto-included; listing it explicitly
			// in select must not cause a DDB ValidationException (duplicate
			// attribute name in ProjectionExpression).
			const result = await ProjectableQueryFacet.query({
				userId: USER,
			}).list({ select: ['userId', 'name'] });

			expect(result.records).toHaveLength(3);
			for (const record of result.records) {
				expect(record.userId).toBe(USER);
				expect(record).toHaveProperty('rowId');
				expect(record).toHaveProperty('name');
				expect(record).not.toHaveProperty('body');
			}
		});

		test('filter combined with select applies both', async () => {
			const result = await ProjectableQueryFacet.query({
				userId: USER,
			}).list({
				filter: ['status', '=', 'queued'],
				select: ['name'],
			});

			expect(result.records.map((r) => r.rowId).sort()).toEqual([
				'r-001',
				'r-002',
			]);
			for (const record of result.records) {
				expect(record).toHaveProperty('name');
				expect(record).not.toHaveProperty('body');
			}
		});

		test('cursor pagination survives a projected query', async () => {
			const first = await ProjectableQueryFacet.query({
				userId: USER,
			}).list({ select: ['name'], limit: 2 });

			expect(first.records).toHaveLength(2);
			expect(first.cursor).toBeDefined();

			const second = await ProjectableQueryFacet.query({
				userId: USER,
			}).list({ select: ['name'], limit: 2, cursor: first.cursor });

			expect(second.records).toHaveLength(1);
			// Collected across both pages, every seeded rowId is visible.
			const seen = [
				...first.records.map((r) => r.rowId),
				...second.records.map((r) => r.rowId),
			].sort();
			expect(seen).toEqual(['r-001', 'r-002', 'r-003']);
		});

		test('type gate: every query method rejects select without pickValidator', () => {
			void (async () => {
				// @ts-expect-error list is gated on PV
				await PlainQueryFacet.query({ userId: USER }).list({
					select: ['name'],
				});
				// @ts-expect-error first is gated
				await PlainQueryFacet.query({ userId: USER }).first({
					select: ['name'],
				});
				// @ts-expect-error equals is gated
				await PlainQueryFacet.query({ userId: USER }).equals(
					{ rowId: 'x' },
					{ select: ['name'] },
				);
				// @ts-expect-error greaterThan is gated
				await PlainQueryFacet.query({ userId: USER }).greaterThan(
					{ rowId: 'x' },
					{ select: ['name'] },
				);
				// @ts-expect-error greaterThanOrEqual is gated
				await PlainQueryFacet.query({ userId: USER }).greaterThanOrEqual(
					{ rowId: 'x' },
					{ select: ['name'] },
				);
				// @ts-expect-error lessThan is gated
				await PlainQueryFacet.query({ userId: USER }).lessThan(
					{ rowId: 'x' },
					{ select: ['name'] },
				);
				// @ts-expect-error lessThanOrEqual is gated
				await PlainQueryFacet.query({ userId: USER }).lessThanOrEqual(
					{ rowId: 'x' },
					{ select: ['name'] },
				);
				// @ts-expect-error beginsWith is gated
				await PlainQueryFacet.query({ userId: USER }).beginsWith(
					{ rowId: 'r-' },
					{ select: ['name'] },
				);
				// @ts-expect-error between is gated
				await PlainQueryFacet.query({ userId: USER }).between(
					{ rowId: 'r-001' },
					{ rowId: 'r-002' },
					{ select: ['name'] },
				);
			});
		});

		test('plain query methods still work without select', async () => {
			await PlainQueryFacet.put({
				userId: USER,
				rowId: 'plain-1',
				status: 'x',
				createdAt: '2024-01-01',
				name: 'N',
				body: 'B',
				count: 1,
			});
			const result = await PlainQueryFacet.query({ userId: USER }).list();
			expect(result.records.some((r) => r.rowId === 'plain-1')).toBe(true);
		});
	});
});

function mockPages(count: number, overrides: Partial<Page> = {}): Page[] {
	const pages: Page[] = [];
	for (let index = 0; index < count; index++) {
		pages.push({
			accessToken: Math.floor(Math.random() * 999999999999)
				.toString(16)
				.padStart(10, '0'),
			pageId: Math.floor(Math.random() * 999999999999)
				.toString(16)
				.padStart(10, '0'),
			pageName: `Page ${index}`,
			tokenStatus: TokenStatus.Active,
			...overrides,
		});
	}
	return pages;
}

function mockPosts(count: number, overrides: Partial<Post> = {}): Post[] {
	const posts: Post[] = [];
	for (let index = 0; index < count; index++) {
		posts.push({
			postId: Math.floor(Math.random() * 999999999999)
				.toString(16)
				.padStart(10, '0'),
			pageId: Math.floor(Math.random() * 999999999999)
				.toString(16)
				.padStart(10, '0'),
			postStatus: PostStatus.Draft,
			...overrides,
		});
	}
	return posts;
}

/**
 * Create the Dynamo DB table for testing
 */
async function createTestTable(): Promise<void> {
	let active = false;

	try {
		await ddb.createTable({
			TableName: tableName,
			AttributeDefinitions: [
				{ AttributeName: 'PK', AttributeType: 'S' },
				{ AttributeName: 'SK', AttributeType: 'S' },
				{ AttributeName: 'GSI1PK', AttributeType: 'S' },
				{ AttributeName: 'GSI1SK', AttributeType: 'S' },
				{ AttributeName: 'GSI2PK', AttributeType: 'S' },
				{ AttributeName: 'GSI2SK', AttributeType: 'S' },
				{ AttributeName: 'GSI3PK', AttributeType: 'S' },
				{ AttributeName: 'GSI3SK', AttributeType: 'S' },
			],
			KeySchema: [
				{ AttributeName: 'PK', KeyType: 'HASH' },
				{ AttributeName: 'SK', KeyType: 'RANGE' },
			],
			BillingMode: 'PAY_PER_REQUEST',
			GlobalSecondaryIndexes: [
				{
					IndexName: 'GSI1',
					KeySchema: [
						{ AttributeName: 'GSI1PK', KeyType: 'HASH' },
						{ AttributeName: 'GSI1SK', KeyType: 'RANGE' },
					],
					Projection: {
						ProjectionType: 'ALL',
					},
				},
				{
					IndexName: 'GSI2',
					KeySchema: [
						{ AttributeName: 'GSI2PK', KeyType: 'HASH' },
						{ AttributeName: 'GSI2SK', KeyType: 'RANGE' },
					],
					Projection: {
						ProjectionType: 'ALL',
					},
				},
				{
					IndexName: 'GSI3',
					KeySchema: [
						{ AttributeName: 'GSI3PK', KeyType: 'HASH' },
						{ AttributeName: 'GSI3SK', KeyType: 'RANGE' },
					],
					Projection: {
						ProjectionType: 'ALL',
					},
				},
			],
		});
	} catch (error) {
		/**
		 * We'll reset the existing table if it already exists.
		 * SDK v3 errors expose the class name on `.name` and via
		 * `instanceof`, not the v2 `.code` field.
		 */
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
