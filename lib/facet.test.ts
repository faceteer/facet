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
