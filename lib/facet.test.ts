import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { Facet } from './facet';
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
		const resourceError = error as any;
		/**
		 * We'll reset the existing table if it already exists
		 */
		if (resourceError.code === 'ResourceInUseException') {
			await ddb.deleteTable({
				TableName: tableName,
			});

			await createTestTable();
		}
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
