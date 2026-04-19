import { DynamoDB, ResourceInUseException } from '@aws-sdk/client-dynamodb';
import { Facet, PickValidator } from './facet';
import { Index } from './keys';
import { wait } from './wait';

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

	test('reserved attribute names fail the type-level constraint', () => {
		// Type-only assertions — the build (which type-checks tests under
		// tsconfig.test.json) is the real check. At runtime this is a no-op.
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
