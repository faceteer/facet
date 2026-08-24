import {
	DynamoDB,
	ResourceInUseException,
	type GetItemCommandInput,
	type UpdateItemCommandInput,
	type UpdateItemInput,
} from '@aws-sdk/client-dynamodb';
import { vi } from 'vitest';
import { toAttributeValue, type AttributeMap } from './converter/converter.js';
import { Facet } from './facet.js';
import { crcShard } from './hash/crc-shard.js';
import { Index } from './keys.js';
import {
	EmptyPatchError,
	PatchIdentityFieldError,
	PatchItemNotFoundError,
	PatchMissingKeyInputsError,
	patchSingleItem,
	type MissingPatchKeyInputs,
	type PatchDemands,
	type PatchFacet,
	type PatchKeyInputGroups,
	type PatchKeyTarget,
} from './patch.js';
import { wait } from './wait.js';

const ddb = new DynamoDB({
	region: 'us-east-1',
	endpoint: 'http://localhost:8000',
});

const tableName = 'TEST';

enum PostStatus {
	Draft = 'draft',
	Queued = 'queued',
	Published = 'published',
}

interface Post {
	pageId: string;
	postId: string;
	postStatus: PostStatus;
	postTitle?: string;
	sendAt?: Date;
	authorId?: string;
	deleteAt?: Date | number | string;
}

const PatchPostFacet = new Facet({
	name: 'PATCH_POST',
	validator: (input: unknown): Post => {
		return input as Post;
	},
	PK: {
		keys: ['pageId'],
		prefix: '#PPAGE',
	},
	SK: {
		keys: ['postId'],
		prefix: '#PPOST',
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
			prefix: '#PSTATUS',
		},
		SK: {
			keys: ['sendAt', 'authorId'],
			prefix: '#PSEND',
		},
		alias: 'byStatus',
	})
	.addIndex({
		index: Index.GSI2,
		PK: {
			keys: ['pageId', 'postStatus'],
			prefix: '#PPAGE',
		},
		SK: {
			keys: ['postId'],
			prefix: '#PPOST',
		},
		alias: 'byPageStatus',
	});

let uniqueCounter = 0;
function mockPost(overrides: Partial<Post> = {}): Post {
	uniqueCounter += 1;
	const unique = `${Math.floor(Math.random() * 999999999999)
		.toString(16)
		.padStart(10, '0')}${uniqueCounter}`;
	return {
		pageId: `page-${unique}`,
		postId: `post-${unique}`,
		postStatus: PostStatus.Draft,
		...overrides,
	};
}

async function putPost(overrides: Partial<Post> = {}): Promise<Post> {
	const post = mockPost(overrides);
	const result = await PatchPostFacet.put(post);
	if (!result.wasSuccessful) {
		throw new Error('Unable to put mock post for test');
	}
	return post;
}

async function rawItem(post: Pick<Post, 'pageId' | 'postId'>) {
	const result = await ddb.getItem({
		TableName: tableName,
		Key: {
			PK: { S: PatchPostFacet.pk(post) },
			SK: { S: PatchPostFacet.sk(post) },
		},
	});
	if (!result.Item) {
		throw new Error('Expected the record to exist in DynamoDB');
	}
	return result.Item;
}

describe('Facet.patch against DynamoDB Local', () => {
	beforeAll(async () => {
		await createTestTable();
	});

	afterEach(() => {
		vi.restoreAllMocks();
	});

	test('patches a plain field without touching any synthetic key', async () => {
		const post = await putPost({
			postTitle: 'Original',
			sendAt: new Date('2026-03-01T00:00:00.000Z'),
			authorId: 'author-1',
		});
		const before = await rawItem(post);
		const getSpy = vi.spyOn(ddb, 'getItem');

		const result = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId },
			{ postTitle: 'Updated' },
		);

		if (!result.wasSuccessful) {
			throw result.error;
		}
		expect(result.record.postTitle).toBe('Updated');
		expect(result.usedFallbackRead).toBe(false);
		expect(getSpy).not.toHaveBeenCalled();

		const after = await rawItem(post);
		expect(after.postTitle.S).toBe('Updated');
		expect(after.GSI1PK).toEqual(before.GSI1PK);
		expect(after.GSI1SK).toEqual(before.GSI1SK);
		expect(after.GSI2PK).toEqual(before.GSI2PK);
		expect(after.GSI2SK).toEqual(before.GSI2SK);
	});

	test('recomputes affected GSI keys in a single round trip when the patch supplies every input', async () => {
		const post = await putPost({
			sendAt: new Date('2026-03-02T00:00:00.000Z'),
		});
		const before = await rawItem(post);
		const getSpy = vi.spyOn(ddb, 'getItem');
		const updateSpy = vi.spyOn(ddb, 'updateItem');

		const result = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId },
			{ postStatus: PostStatus.Published },
		);

		if (!result.wasSuccessful) {
			throw result.error;
		}
		expect(result.usedFallbackRead).toBe(false);
		expect(getSpy).not.toHaveBeenCalled();
		expect(updateSpy).toHaveBeenCalledTimes(1);

		const after = await rawItem(post);
		expect(after.GSI1PK.S).toBe(
			PatchPostFacet.byStatus.pk({
				postStatus: PostStatus.Published,
				postId: post.postId,
			}),
		);
		expect(after.GSI2PK.S).toBe(
			PatchPostFacet.byPageStatus.pk({
				pageId: post.pageId,
				postStatus: PostStatus.Published,
			}),
		);
		// The sort keys take no input from the patch and stay put.
		expect(after.GSI1SK).toEqual(before.GSI1SK);
		expect(after.GSI2SK).toEqual(before.GSI2SK);
	});

	test('a patched row lands in the correct shard of a sharded GSI', async () => {
		const post = await putPost({
			sendAt: new Date('2026-03-03T00:00:00.000Z'),
			authorId: 'author-shard',
		});

		const result = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId },
			{ postStatus: PostStatus.Queued },
		);
		if (!result.wasSuccessful) {
			throw result.error;
		}

		const shard = parseInt(crcShard(post.postId, 4), 16);
		const queried = await PatchPostFacet.byStatus
			.query({ postStatus: PostStatus.Queued }, shard)
			.list();
		expect(
			queried.records.some((record) => record.postId === post.postId),
		).toBe(true);

		// The row left its old partition when the key moved.
		const stillDraft = await PatchPostFacet.byStatus
			.query({ postStatus: PostStatus.Draft }, shard)
			.list();
		expect(
			stillDraft.records.some((record) => record.postId === post.postId),
		).toBe(false);
	});

	test('reads the record to resolve a missing key input and recomputes from its current values', async () => {
		const post = await putPost({
			sendAt: new Date('2026-03-04T00:00:00.000Z'),
			authorId: 'author-4',
		});
		const before = await rawItem(post);
		const getSpy = vi.spyOn(ddb, 'getItem');
		const updateSpy = vi.spyOn(ddb, 'updateItem');
		const newSendAt = new Date('2026-04-04T00:00:00.000Z');

		const result = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId },
			{ sendAt: newSendAt },
			{ missingKeyInputs: 'read' },
		);

		if (!result.wasSuccessful) {
			throw result.error;
		}
		expect(result.usedFallbackRead).toBe(true);
		expect(getSpy).toHaveBeenCalledTimes(1);
		expect(updateSpy).toHaveBeenCalledTimes(1);

		const after = await rawItem(post);
		expect(after.GSI1SK.S).toBe(
			PatchPostFacet.byStatus.sk({
				sendAt: newSendAt,
				authorId: post.authorId,
			}),
		);
		expect(after.GSI1PK).toEqual(before.GSI1PK);
		expect(after.authorId.S).toBe(post.authorId);
	});

	test('strict mode reports which fields and keys are involved when the compiler cannot see them', async () => {
		const post = await putPost({
			sendAt: new Date('2026-03-05T00:00:00.000Z'),
			authorId: 'author-5',
		});
		const getSpy = vi.spyOn(ddb, 'getItem');
		const updateSpy = vi.spyOn(ddb, 'updateItem');

		/**
		 * A facet widened to a type without its index accessors erases
		 * what the strict overload checks at compile time, so the same
		 * incomplete patch compiles here and exercises the runtime
		 * backstop instead.
		 */
		const ErasedPostFacet: Facet<Post, 'pageId', 'postId'> = PatchPostFacet;
		const result = await ErasedPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId },
			{ sendAt: new Date('2026-04-05T00:00:00.000Z') },
		);

		expect(result.wasSuccessful).toBe(false);
		if (result.wasSuccessful) {
			throw new Error('Expected the patch to report a failure');
		}
		expect(result.error).toBeInstanceOf(PatchMissingKeyInputsError);
		const error = result.error as PatchMissingKeyInputsError;
		expect(error.fields).toEqual(['authorId']);
		expect(error.attributeNames).toEqual(['GSI1SK']);
		expect(result.usedFallbackRead).toBe(false);
		expect(getSpy).not.toHaveBeenCalled();
		expect(updateSpy).not.toHaveBeenCalled();
	});

	test('supplying the missing key input in the query keeps the patch to one round trip', async () => {
		const post = await putPost({
			sendAt: new Date('2026-03-06T00:00:00.000Z'),
			authorId: 'author-6',
		});
		const getSpy = vi.spyOn(ddb, 'getItem');
		const newSendAt = new Date('2026-04-06T00:00:00.000Z');

		const result = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId, authorId: post.authorId },
			{ sendAt: newSendAt },
			{ missingKeyInputs: 'strict' },
		);

		if (!result.wasSuccessful) {
			throw result.error;
		}
		expect(getSpy).not.toHaveBeenCalled();
		const after = await rawItem(post);
		expect(after.GSI1SK.S).toBe(
			PatchPostFacet.byStatus.sk({
				sendAt: newSendAt,
				authorId: post.authorId,
			}),
		);
	});

	test('a Date query hint feeds the key recompute and is guarded with the stored representation', async () => {
		const sendAt = new Date('2026-03-15T00:00:00.000Z');
		const post = await putPost({ sendAt, authorId: 'author-15' });
		const getSpy = vi.spyOn(ddb, 'getItem');

		const result = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId, sendAt },
			{ authorId: 'author-15-new' },
		);

		if (!result.wasSuccessful) {
			throw result.error;
		}
		expect(getSpy).not.toHaveBeenCalled();
		const after = await rawItem(post);
		expect(after.GSI1SK.S).toBe(
			PatchPostFacet.byStatus.sk({ sendAt, authorId: 'author-15-new' }),
		);
	});

	test('an explicitly undefined query hint asserts the attribute is absent', async () => {
		const post = await putPost();
		const getSpy = vi.spyOn(ddb, 'getItem');
		const sendAt = new Date('2026-04-16T00:00:00.000Z');

		const result = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId, authorId: undefined },
			{ sendAt },
		);

		if (!result.wasSuccessful) {
			throw result.error;
		}
		expect(getSpy).not.toHaveBeenCalled();
		const after = await rawItem(post);
		expect(after.GSI1SK.S).toBe(PatchPostFacet.byStatus.sk({ sendAt }));
	});

	test('a fallback read resolves identity inputs alongside read inputs', async () => {
		const post = await putPost({
			sendAt: new Date('2026-03-17T00:00:00.000Z'),
			authorId: 'author-17',
		});
		const getSpy = vi.spyOn(ddb, 'getItem');
		const newSendAt = new Date('2026-04-17T00:00:00.000Z');

		const result = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId },
			{ postStatus: PostStatus.Published, sendAt: newSendAt },
			{ missingKeyInputs: 'read' },
		);

		if (!result.wasSuccessful) {
			throw result.error;
		}
		expect(getSpy).toHaveBeenCalledTimes(1);
		const after = await rawItem(post);
		expect(after.GSI1PK.S).toBe(
			PatchPostFacet.byStatus.pk({
				postStatus: PostStatus.Published,
				postId: post.postId,
			}),
		);
		expect(after.GSI2PK.S).toBe(
			PatchPostFacet.byPageStatus.pk({
				pageId: post.pageId,
				postStatus: PostStatus.Published,
			}),
		);
		expect(after.GSI1SK.S).toBe(
			PatchPostFacet.byStatus.sk({
				sendAt: newSendAt,
				authorId: post.authorId,
			}),
		);
	});

	test('a patch carrying a reserved attribute name reports the failure', async () => {
		const post = await putPost();
		const updateSpy = vi.spyOn(ddb, 'updateItem');

		const result = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId },
			{ GSI1PK: 'forged' } as unknown as { postTitle: string },
		);

		expect(result.wasSuccessful).toBe(false);
		if (result.wasSuccessful) {
			throw new Error('Expected the patch to report a failure');
		}
		expect((result.error as Error).message).toContain(
			'reserved attribute "GSI1PK"',
		);
		expect(updateSpy).not.toHaveBeenCalled();
	});

	test('a stale query hint fails its guard and leaves the stored key untouched', async () => {
		const post = await putPost({
			sendAt: new Date('2026-03-07T00:00:00.000Z'),
			authorId: 'author-real',
		});
		const before = await rawItem(post);
		const getSpy = vi.spyOn(ddb, 'getItem');

		const result = await PatchPostFacet.patch(
			{
				pageId: post.pageId,
				postId: post.postId,
				authorId: 'author-stale',
			},
			{ sendAt: new Date('2026-04-07T00:00:00.000Z') },
		);

		expect(getSpy).not.toHaveBeenCalled();
		expect(result.wasSuccessful).toBe(false);
		if (result.wasSuccessful) {
			throw new Error('Expected the patch to report a failure');
		}
		expect((result.error as Error).name).toBe(
			'ConditionalCheckFailedException',
		);
		// A guard failure on an existing row carries the conflicting
		// record, same as a caller condition failure.
		expect(result.conflictingItemRaw).toBeDefined();
		expect(result.conflictingItem?.authorId).toBe('author-real');

		const after = await rawItem(post);
		expect(after.GSI1SK).toEqual(before.GSI1SK);
		expect(after.sendAt).toEqual(before.sendAt);
	});

	test('a fallback read that returns stale data fails its guard instead of writing a stale key', async () => {
		const post = await putPost({
			sendAt: new Date('2026-03-08T00:00:00.000Z'),
			authorId: 'author-current',
		});
		const before = await rawItem(post);

		/**
		 * Simulate a stale eventually consistent read: the wrapped
		 * client returns an older authorId than the one stored, as if
		 * a concurrent writer had committed between the read and the
		 * update.
		 */
		const staleDdb = {
			getItem: async (input: GetItemCommandInput) => {
				const result = await ddb.getItem(input);
				if (result.Item?.authorId) {
					result.Item.authorId = { S: 'author-outdated' };
				}
				return result;
			},
			updateItem: (input: UpdateItemCommandInput) => ddb.updateItem(input),
		} as unknown as DynamoDB;

		const StalePostFacet = new Facet({
			name: 'PATCH_POST',
			validator: (input: unknown): Post => input as Post,
			PK: { keys: ['pageId'], prefix: '#PPAGE' },
			SK: { keys: ['postId'], prefix: '#PPOST' },
			connection: { dynamoDb: staleDdb, tableName },
			ttl: 'deleteAt',
		})
			.addIndex({
				index: Index.GSI1,
				PK: {
					keys: ['postStatus'],
					shard: { count: 4, keys: ['postId'] },
					prefix: '#PSTATUS',
				},
				SK: { keys: ['sendAt', 'authorId'], prefix: '#PSEND' },
			})
			.addIndex({
				index: Index.GSI2,
				PK: { keys: ['pageId', 'postStatus'], prefix: '#PPAGE' },
				SK: { keys: ['postId'], prefix: '#PPOST' },
			});

		const result = await StalePostFacet.patch(
			{ pageId: post.pageId, postId: post.postId },
			{ sendAt: new Date('2026-04-08T00:00:00.000Z') },
			{ missingKeyInputs: 'read' },
		);

		expect(result.wasSuccessful).toBe(false);
		if (result.wasSuccessful) {
			throw new Error('Expected the patch to report a failure');
		}
		expect((result.error as Error).name).toBe(
			'ConditionalCheckFailedException',
		);
		expect(result.usedFallbackRead).toBe(true);
		expect(result.conflictingItemRaw).toBeDefined();
		expect(result.conflictingItem?.authorId).toBe('author-current');

		const after = await rawItem(post);
		expect(after.GSI1SK).toEqual(before.GSI1SK);
	});

	test('a caller condition gates the patch and a failure carries the conflicting record', async () => {
		const post = await putPost({ postTitle: 'Conditional' });

		const allowed = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId },
			{ postTitle: 'First edit' },
			{ condition: ['postStatus', '=', PostStatus.Draft] },
		);
		if (!allowed.wasSuccessful) {
			throw allowed.error;
		}
		expect(allowed.record.postTitle).toBe('First edit');

		const blocked = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId },
			{ postTitle: 'Second edit' },
			{ condition: ['postStatus', '=', PostStatus.Queued] },
		);
		expect(blocked.wasSuccessful).toBe(false);
		if (blocked.wasSuccessful) {
			throw new Error('Expected the patch to report a failure');
		}
		expect((blocked.error as Error).name).toBe(
			'ConditionalCheckFailedException',
		);
		expect(blocked.conflictingItemRaw).toBeDefined();
		expect(blocked.conflictingItem?.postStatus).toBe(PostStatus.Draft);
		expect(blocked.conflictingItem?.postTitle).toBe('First edit');
	});

	test('patching a missing record reports a failure instead of upserting', async () => {
		const ghost = mockPost();

		const plain = await PatchPostFacet.patch(
			{ pageId: ghost.pageId, postId: ghost.postId },
			{ postTitle: 'Nope' },
		);
		expect(plain.wasSuccessful).toBe(false);
		if (plain.wasSuccessful) {
			throw new Error('Expected the patch to report a failure');
		}
		expect((plain.error as Error).name).toBe('ConditionalCheckFailedException');
		expect(plain.conflictingItemRaw).toBeUndefined();
		expect(plain.usedFallbackRead).toBe(false);

		const viaRead = await PatchPostFacet.patch(
			{ pageId: ghost.pageId, postId: ghost.postId },
			{ sendAt: new Date('2026-04-09T00:00:00.000Z') },
			{ missingKeyInputs: 'read' },
		);
		expect(viaRead.wasSuccessful).toBe(false);
		if (viaRead.wasSuccessful) {
			throw new Error('Expected the patch to report a failure');
		}
		expect(viaRead.error).toBeInstanceOf(PatchItemNotFoundError);
		expect(viaRead.usedFallbackRead).toBe(true);

		const gone = await ddb.getItem({
			TableName: tableName,
			Key: {
				PK: { S: PatchPostFacet.pk(ghost) },
				SK: { S: PatchPostFacet.sk(ghost) },
			},
		});
		expect(gone.Item).toBeUndefined();
	});

	test('a record whose optional key inputs are all absent patches without a false not-found', async () => {
		const post = await putPost();
		const sendAt = new Date('2026-04-10T00:00:00.000Z');

		const result = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId },
			{ sendAt },
			{ missingKeyInputs: 'read' },
		);

		if (!result.wasSuccessful) {
			throw result.error;
		}
		const after = await rawItem(post);
		expect(after.GSI1SK.S).toBe(PatchPostFacet.byStatus.sk({ sendAt }));
		expect(after.authorId).toBeUndefined();
	});

	test('undefined removes the attribute and rebuilds dependent keys down to their prefix', async () => {
		const post = await putPost({
			postTitle: 'Removable',
			sendAt: new Date('2026-03-11T00:00:00.000Z'),
			authorId: 'author-11',
		});

		const plain = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId },
			{ postTitle: undefined },
		);
		if (!plain.wasSuccessful) {
			throw plain.error;
		}
		expect(plain.record.postTitle).toBeUndefined();
		let after = await rawItem(post);
		expect(after.postTitle).toBeUndefined();

		const keyed = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId },
			{ sendAt: undefined, authorId: undefined },
		);
		if (!keyed.wasSuccessful) {
			throw keyed.error;
		}
		after = await rawItem(post);
		expect(after.sendAt).toBeUndefined();
		expect(after.authorId).toBeUndefined();
		expect(after.GSI1SK.S).toBe(PatchPostFacet.byStatus.sk({}));
	});

	test('patching the TTL field rewrites the synthetic ttl attribute like put does', async () => {
		const post = await putPost();
		const key = { pageId: post.pageId, postId: post.postId };

		const viaDate = await PatchPostFacet.patch(key, {
			deleteAt: new Date('2030-01-01T00:00:00.000Z'),
		});
		if (!viaDate.wasSuccessful) {
			throw viaDate.error;
		}
		let after = await rawItem(post);
		expect(after.ttl.N).toBe('1893456000');
		expect(after.deleteAt.S).toBe('2030-01-01T00:00:00.000Z');

		const viaNumber = await PatchPostFacet.patch(key, {
			deleteAt: 1893456111,
		});
		if (!viaNumber.wasSuccessful) {
			throw viaNumber.error;
		}
		after = await rawItem(post);
		expect(after.ttl.N).toBe('1893456111');
		expect(after.deleteAt.N).toBe('1893456111');

		const viaJunk = await PatchPostFacet.patch(key, { deleteAt: 'soon' });
		if (!viaJunk.wasSuccessful) {
			throw viaJunk.error;
		}
		after = await rawItem(post);
		expect(after.ttl).toBeUndefined();
		expect(after.deleteAt.S).toBe('soon');
	});

	test('an empty patch reports EmptyPatchError without a network call', async () => {
		const post = await putPost();
		const getSpy = vi.spyOn(ddb, 'getItem');
		const updateSpy = vi.spyOn(ddb, 'updateItem');

		const result = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId },
			{},
		);

		expect(result.wasSuccessful).toBe(false);
		if (result.wasSuccessful) {
			throw new Error('Expected the patch to report a failure');
		}
		expect(result.error).toBeInstanceOf(EmptyPatchError);
		expect(getSpy).not.toHaveBeenCalled();
		expect(updateSpy).not.toHaveBeenCalled();
	});

	test('patching a base shard key reports PatchIdentityFieldError without a network call', async () => {
		interface Metric {
			metricId: string;
			groupId: string;
			value?: number;
		}
		const MetricFacet = new Facet({
			name: 'PATCH_METRIC',
			validator: (input: unknown): Metric => input as Metric,
			PK: {
				keys: ['metricId'],
				shard: { count: 4, keys: ['groupId'] },
				prefix: '#PMETRIC',
			},
			SK: { keys: [], prefix: '#PMETRIC' },
			connection: { dynamoDb: ddb, tableName },
		});
		const getSpy = vi.spyOn(ddb, 'getItem');
		const updateSpy = vi.spyOn(ddb, 'updateItem');

		const result = await MetricFacet.patch(
			{ metricId: 'm1', groupId: 'g1' },
			{ groupId: 'g2', value: 5 },
		);

		expect(result.wasSuccessful).toBe(false);
		if (result.wasSuccessful) {
			throw new Error('Expected the patch to report a failure');
		}
		expect(result.error).toBeInstanceOf(PatchIdentityFieldError);
		expect((result.error as PatchIdentityFieldError).fields).toEqual([
			'groupId',
		]);
		expect(getSpy).not.toHaveBeenCalled();
		expect(updateSpy).not.toHaveBeenCalled();
	});

	test('a patch that lands but fails the validator reports the failure while the write persists', async () => {
		interface Doc {
			docId: string;
			body: string;
		}
		const DocFacet = new Facet({
			name: 'PATCH_DOC',
			validator: (input: unknown): Doc => {
				const record = input as Record<string, unknown>;
				if (typeof record.body !== 'string') {
					throw new Error('body is required');
				}
				return record as unknown as Doc;
			},
			PK: { keys: ['docId'], prefix: '#PDOC' },
			SK: { keys: [], prefix: '#PDOC' },
			connection: { dynamoDb: ddb, tableName },
		});
		const doc: Doc = { docId: 'patch-doc-1', body: 'hello' };
		const put = await DocFacet.put(doc);
		if (!put.wasSuccessful) {
			throw put.error;
		}

		const result = await DocFacet.patch(
			{ docId: doc.docId },
			{ body: undefined },
		);

		expect(result.wasSuccessful).toBe(false);
		if (result.wasSuccessful) {
			throw new Error('Expected the patch to report a failure');
		}
		expect((result.error as Error).message).toBe('body is required');

		const after = await ddb.getItem({
			TableName: tableName,
			Key: { PK: { S: DocFacet.pk(doc) }, SK: { S: DocFacet.sk(doc) } },
		});
		expect(after.Item).toBeDefined();
		expect(after.Item?.body).toBeUndefined();
	});

	test('the fallback read stays key-faithful on a unix dateFormat facet with a Date key input', async () => {
		interface UnixEvent {
			eventId: string;
			kind: string;
			happenedAt?: Date;
			label?: string;
		}
		const UnixFacet = new Facet({
			name: 'PATCH_UNIX',
			dateFormat: 'unix',
			validator: (input: unknown): UnixEvent => {
				const record = input as Record<string, unknown>;
				// Stored unix dates unmarshall as epoch-second strings;
				// restore the Date the key builders need.
				const happenedAt =
					typeof record.happenedAt === 'string'
						? new Date(Number(record.happenedAt) * 1000)
						: (record.happenedAt as Date | undefined);
				return { ...record, happenedAt } as UnixEvent;
			},
			PK: { keys: ['eventId'], prefix: '#PUNIX' },
			SK: { keys: [], prefix: '#PUNIX' },
			connection: { dynamoDb: ddb, tableName },
		}).addIndex({
			index: Index.GSI1,
			PK: { keys: ['kind'], prefix: '#PKIND' },
			SK: { keys: ['happenedAt', 'label'], prefix: '#PAT' },
		});

		const happenedAt = new Date('2026-01-02T03:04:05.000Z');
		const put = await UnixFacet.put({
			eventId: 'unix-1',
			kind: 'deploy',
			happenedAt,
			label: 'x',
		});
		if (!put.wasSuccessful) {
			throw put.error;
		}

		const result = await UnixFacet.patch(
			{ eventId: 'unix-1' },
			{ label: 'y' },
			{ missingKeyInputs: 'read' },
		);
		if (!result.wasSuccessful) {
			throw result.error;
		}

		const after = await ddb.getItem({
			TableName: tableName,
			Key: {
				PK: { S: UnixFacet.pk({ eventId: 'unix-1' }) },
				SK: { S: UnixFacet.sk({}) },
			},
		});
		// The key keeps buildKey's ISO rendering even though the raw
		// attribute stores epoch seconds.
		expect(after.Item?.GSI1SK.S).toBe(
			UnixFacet.GSI1.sk({ happenedAt, label: 'y' }),
		);
		expect(after.Item?.GSI1SK.S).toContain('2026-01-02T03:04:05.000Z');
		expect(after.Item?.happenedAt.S).toBe(
			`${Math.floor(happenedAt.getTime() / 1000)}`,
		);
	});

	test('the fallback read stays key-faithful with convertEmptyValues and an empty-string key input', async () => {
		interface Tag {
			tagId: string;
			note?: string;
			label?: string;
		}
		const TagFacet = new Facet({
			name: 'PATCH_TAG',
			convertEmptyValues: true,
			validator: (input: unknown): Tag => {
				const record = input as Record<string, unknown>;
				// Empty strings store as NULL under convertEmptyValues;
				// restore them so keys keep their empty segment.
				return {
					...record,
					note: record.note === null ? '' : record.note,
				} as Tag;
			},
			PK: { keys: ['tagId'], prefix: '#PTAG' },
			SK: { keys: [], prefix: '#PTAG' },
			connection: { dynamoDb: ddb, tableName },
		}).addIndex({
			index: Index.GSI2,
			PK: { keys: ['tagId'], prefix: '#PTAGX' },
			SK: { keys: ['note', 'label'], prefix: '#PNOTE' },
		});

		const put = await TagFacet.put({ tagId: 'tag-1', note: '', label: 'a' });
		if (!put.wasSuccessful) {
			throw put.error;
		}
		const before = await ddb.getItem({
			TableName: tableName,
			Key: {
				PK: { S: TagFacet.pk({ tagId: 'tag-1' }) },
				SK: { S: TagFacet.sk({}) },
			},
		});
		expect(before.Item?.note.NULL).toBe(true);

		const result = await TagFacet.patch(
			{ tagId: 'tag-1' },
			{ label: 'b' },
			{ missingKeyInputs: 'read' },
		);
		if (!result.wasSuccessful) {
			throw result.error;
		}

		const after = await ddb.getItem({
			TableName: tableName,
			Key: {
				PK: { S: TagFacet.pk({ tagId: 'tag-1' }) },
				SK: { S: TagFacet.sk({}) },
			},
		});
		// The empty segment survives: prefix, empty note, then label.
		expect(after.Item?.GSI2SK.S).toBe(
			TagFacet.GSI2.sk({ note: '', label: 'b' }),
		);
		expect(after.Item?.GSI2SK.S).toBe('#PNOTE__b');
	});

	test('a conflicting record the validator rejects still surfaces the raw item', async () => {
		interface StrictDoc {
			strictDocId: string;
			body?: string;
			flag?: string;
		}
		const StrictDocFacet = new Facet({
			name: 'PATCH_STRICT_DOC',
			validator: (input: unknown): StrictDoc => {
				const record = input as Record<string, unknown>;
				if (typeof record.body !== 'string') {
					throw new Error('body is required');
				}
				return record as unknown as StrictDoc;
			},
			PK: { keys: ['strictDocId'], prefix: '#PSDOC' },
			SK: { keys: [], prefix: '#PSDOC' },
			connection: { dynamoDb: ddb, tableName },
		});
		// Seed a row the validator rejects, as written by an older schema
		// that had no body field.
		await ddb.putItem({
			TableName: tableName,
			Item: {
				PK: { S: StrictDocFacet.pk({ strictDocId: 'strict-doc-1' }) },
				SK: { S: StrictDocFacet.sk({}) },
				facet: { S: 'PATCH_STRICT_DOC' },
				strictDocId: { S: 'strict-doc-1' },
			},
		});

		const result = await StrictDocFacet.patch(
			{ strictDocId: 'strict-doc-1' },
			{ flag: 'x' },
			{ condition: ['body', 'exists'] },
		);

		expect(result.wasSuccessful).toBe(false);
		if (result.wasSuccessful) {
			throw new Error('Expected the patch to report a failure');
		}
		// The conflicting item fails the validator, so the parsed field
		// stays unset while the raw item remains available, and the call
		// resolves instead of throwing.
		expect((result.error as Error).name).toBe(
			'ConditionalCheckFailedException',
		);
		expect(result.conflictingItemRaw).toBeDefined();
		expect(result.conflictingItem).toBeUndefined();
	});

	test('a non-conditional error from the update reports without conflict details', async () => {
		const post = await putPost();
		const boom = new Error('socket hang up');
		vi.spyOn(ddb, 'updateItem').mockImplementationOnce(() => {
			throw boom;
		});

		const result = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId },
			{ postTitle: 'Never lands' },
		);

		expect(result.wasSuccessful).toBe(false);
		if (result.wasSuccessful) {
			throw new Error('Expected the patch to report a failure');
		}
		expect(result.error).toBe(boom);
		expect(result.conflictingItem).toBeUndefined();
		expect(result.conflictingItemRaw).toBeUndefined();
		expect(result.usedFallbackRead).toBe(false);
	});

	test('a failed fallback read reports the error with usedFallbackRead set', async () => {
		const post = await putPost();
		const boom = new Error('read timed out');
		vi.spyOn(ddb, 'getItem').mockImplementationOnce(() => {
			throw boom;
		});

		const result = await PatchPostFacet.patch(
			{ pageId: post.pageId, postId: post.postId },
			{ sendAt: new Date('2026-05-01T00:00:00.000Z') },
			{ missingKeyInputs: 'read' },
		);

		expect(result.wasSuccessful).toBe(false);
		if (result.wasSuccessful) {
			throw new Error('Expected the patch to report a failure');
		}
		expect(result.error).toBe(boom);
		expect(result.usedFallbackRead).toBe(true);
	});

	test('numeric and boolean key inputs rebuild keys and guard against DynamoDB', async () => {
		interface Task {
			taskId: string;
			queue?: string;
			priority?: number;
			pinned?: boolean;
		}
		const TaskFacet = new Facet({
			name: 'PATCH_TASK',
			validator: (input: unknown): Task => input as Task,
			PK: { keys: ['taskId'], prefix: '#PTASK' },
			SK: { keys: [], prefix: '#PTASK' },
			connection: { dynamoDb: ddb, tableName },
		}).addIndex({
			index: Index.GSI3,
			PK: { keys: ['queue'], prefix: '#PTQ' },
			SK: { keys: ['priority', 'pinned'], prefix: '#PTP' },
		});
		const put = await TaskFacet.put({
			taskId: 'task-num-1',
			queue: 'q1',
			priority: 5,
			pinned: true,
		});
		if (!put.wasSuccessful) {
			throw put.error;
		}
		const rawKey = {
			PK: { S: TaskFacet.pk({ taskId: 'task-num-1' }) },
			SK: { S: TaskFacet.sk({}) },
		};

		// The read fallback resolves priority as a number and guards it
		// with the stored N value.
		const viaRead = await TaskFacet.patch(
			{ taskId: 'task-num-1' },
			{ pinned: false },
			{ missingKeyInputs: 'read' },
		);
		if (!viaRead.wasSuccessful) {
			throw viaRead.error;
		}
		let item = (await ddb.getItem({ TableName: tableName, Key: rawKey })).Item;
		expect(item?.GSI3SK.S).toBe(
			TaskFacet.GSI3.sk({ priority: 5, pinned: false }),
		);

		// A numeric hint marshals to the same N guard in one round trip.
		const getSpy = vi.spyOn(ddb, 'getItem');
		const viaHint = await TaskFacet.patch(
			{ taskId: 'task-num-1', priority: 5 },
			{ pinned: true },
		);
		if (!viaHint.wasSuccessful) {
			throw viaHint.error;
		}
		expect(getSpy).not.toHaveBeenCalled();
		item = (await ddb.getItem({ TableName: tableName, Key: rawKey })).Item;
		expect(item?.GSI3SK.S).toBe(
			TaskFacet.GSI3.sk({ priority: 5, pinned: true }),
		);
		expect(item?.GSI3SK.S).toContain('_5_true');

		// A stale numeric hint fails its guard.
		const stale = await TaskFacet.patch(
			{ taskId: 'task-num-1', priority: 6 },
			{ pinned: false },
		);
		expect(stale.wasSuccessful).toBe(false);
		if (stale.wasSuccessful) {
			throw new Error('Expected the patch to report a failure');
		}
		expect((stale.error as Error).name).toBe('ConditionalCheckFailedException');
	});

	test('patching a GSI shard key input moves the row to its new shard', async () => {
		interface Sensor {
			sensorId: string;
			zone?: string;
			deviceId?: string;
		}
		const SensorFacet = new Facet({
			name: 'PATCH_SENSOR',
			validator: (input: unknown): Sensor => input as Sensor,
			PK: { keys: ['sensorId'], prefix: '#PSEN' },
			SK: { keys: [], prefix: '#PSEN' },
			connection: { dynamoDb: ddb, tableName },
		}).addIndex({
			index: Index.GSI3,
			PK: {
				keys: ['zone'],
				shard: { count: 8, keys: ['deviceId'] },
				prefix: '#PSZ',
			},
			SK: { keys: ['sensorId'], prefix: '#PSZ' },
		});
		const oldShard = parseInt(crcShard('device-a', 8), 16);
		const newShard = parseInt(crcShard('device-b', 8), 16);
		expect(oldShard).not.toBe(newShard);

		const put = await SensorFacet.put({
			sensorId: 'sensor-1',
			zone: 'z1',
			deviceId: 'device-a',
		});
		if (!put.wasSuccessful) {
			throw put.error;
		}

		const result = await SensorFacet.patch(
			{ sensorId: 'sensor-1', zone: 'z1' },
			{ deviceId: 'device-b' },
		);
		if (!result.wasSuccessful) {
			throw result.error;
		}

		const inNew = await SensorFacet.GSI3.query({ zone: 'z1' }, newShard).list();
		expect(inNew.records.some((record) => record.sensorId === 'sensor-1')).toBe(
			true,
		);
		const inOld = await SensorFacet.GSI3.query({ zone: 'z1' }, oldShard).list();
		expect(inOld.records.some((record) => record.sensorId === 'sensor-1')).toBe(
			false,
		);
	});

	test('a patch backfills index keys on a row that predates addIndex', async () => {
		interface Order {
			orderId: string;
			status?: string;
			region?: string;
			carrier?: string;
		}
		const BareOrderFacet = new Facet({
			name: 'PATCH_ORDER',
			validator: (input: unknown): Order => input as Order,
			PK: { keys: ['orderId'], prefix: '#PORD' },
			SK: { keys: [], prefix: '#PORD' },
			connection: { dynamoDb: ddb, tableName },
		});
		const IndexedOrderFacet = new Facet({
			name: 'PATCH_ORDER',
			validator: (input: unknown): Order => input as Order,
			PK: { keys: ['orderId'], prefix: '#PORD' },
			SK: { keys: [], prefix: '#PORD' },
			connection: { dynamoDb: ddb, tableName },
		}).addIndex({
			index: Index.GSI3,
			PK: { keys: ['status'], prefix: '#PORS' },
			SK: { keys: ['region', 'carrier'], prefix: '#PORR' },
		});

		// A row written before the index existed has no GSI3 attributes.
		const put = await BareOrderFacet.put({ orderId: 'order-legacy-1' });
		if (!put.wasSuccessful) {
			throw put.error;
		}
		const legacyKey = {
			PK: { S: IndexedOrderFacet.pk({ orderId: 'order-legacy-1' }) },
			SK: { S: IndexedOrderFacet.sk({}) },
		};
		let item = (await ddb.getItem({ TableName: tableName, Key: legacyKey }))
			.Item;
		expect(item?.GSI3PK).toBeUndefined();
		expect(item?.GSI3SK).toBeUndefined();

		// A patch supplying every input of both keys backfills them and
		// makes the row queryable through the new index.
		const backfill = await IndexedOrderFacet.patch(
			{ orderId: 'order-legacy-1' },
			{ status: 'open', region: 'eu', carrier: 'dhl' },
		);
		if (!backfill.wasSuccessful) {
			throw backfill.error;
		}
		const queried = await IndexedOrderFacet.GSI3.query({
			status: 'open',
		}).list();
		expect(
			queried.records.some((record) => record.orderId === 'order-legacy-1'),
		).toBe(true);

		// The read fallback tolerates a legacy row whose optional inputs
		// are absent and recomputes only the touched key.
		const put2 = await BareOrderFacet.put({ orderId: 'order-legacy-2' });
		if (!put2.wasSuccessful) {
			throw put2.error;
		}
		const partial = await IndexedOrderFacet.patch(
			{ orderId: 'order-legacy-2' },
			{ region: 'apac' },
			{ missingKeyInputs: 'read' },
		);
		if (!partial.wasSuccessful) {
			throw partial.error;
		}
		item = (
			await ddb.getItem({
				TableName: tableName,
				Key: {
					PK: { S: IndexedOrderFacet.pk({ orderId: 'order-legacy-2' }) },
					SK: { S: IndexedOrderFacet.sk({}) },
				},
			})
		).Item;
		expect(item?.GSI3SK.S).toBe(IndexedOrderFacet.GSI3.sk({ region: 'apac' }));
		expect(item?.GSI3PK).toBeUndefined();
	});

	test('patch response and parameters hold their compile-time contracts', () => {
		void (async () => {
			const result = await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				{ postTitle: 'new title', postStatus: PostStatus.Queued },
			);

			if (result.wasSuccessful) {
				// Narrowed to PatchSuccess<Post>: record is a full Post.
				const title: string | undefined = result.record.postTitle;
				void title;
				// @ts-expect-error error does not exist on the success branch
				void result.error;
			} else {
				// Narrowed to PatchFailure<Post>: error is required.
				void result.error;
				// @ts-expect-error record does not exist on the failure branch
				void result.record;
			}

			// PK and SK fields are excluded from the patch parameter.
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				// @ts-expect-error pageId composes the PK and is not patchable
				{ pageId: 'other' },
			);
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				// @ts-expect-error postId composes the SK and is not patchable
				{ postId: 'other' },
			);

			// Patched fields keep their declared value types.
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				// @ts-expect-error postStatus must be a PostStatus
				{ postStatus: 5 },
			);
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				// @ts-expect-error unknownField does not exist on Post
				{ unknownField: true },
			);

			// Conditions type-check against the full model, including
			// fields the patch does not touch.
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				{ postStatus: PostStatus.Queued },
				{ condition: ['postTitle', 'exists'] },
			);
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				{ postStatus: PostStatus.Queued },
				// @ts-expect-error startsWith is not a condition operator
				{ condition: ['postTitle', 'startsWith', 'x'] },
			);

			// Option literals are constrained to the documented values.
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				{ postTitle: 't' },
				{ missingKeyInputs: 'read' },
			);
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				{ postTitle: 't' },
				{ missingKeyInputs: 'strict' },
			);
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				{ postTitle: 't' },
				// @ts-expect-error auto is not a valid missingKeyInputs value
				{ missingKeyInputs: 'auto' },
			);
			const widenMode = (mode: 'read' | 'strict'): string => mode;
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				{ postTitle: 't' },
				// @ts-expect-error a widened string does not select a mode
				{ missingKeyInputs: widenMode('read') },
			);

			// Both response branches expose the fallback-read flag.
			const flag: boolean = result.usedFallbackRead;
			void flag;
		});

		expect<0>(0 satisfies 0).toBe(0);
	});

	test('patchInputs reports the extra fields each affected key needs', () => {
		expect(PatchPostFacet.patchInputs(['sendAt'])).toEqual({
			GSI1SK: ['authorId'],
		});
		expect(PatchPostFacet.patchInputs(['authorId'])).toEqual({
			GSI1SK: ['sendAt'],
		});
		// Base key fields cover GSI1PK's shard key and GSI2PK's inputs.
		expect(PatchPostFacet.patchInputs(['postStatus'])).toEqual({});
		expect(PatchPostFacet.patchInputs(['postTitle'])).toEqual({});
		expect(PatchPostFacet.patchInputs(['sendAt', 'authorId'])).toEqual({});
		expect(PatchPostFacet.patchInputs(['postStatus', 'sendAt'])).toEqual({
			GSI1SK: ['authorId'],
		});

		// Non-identity shard keys count as required inputs.
		interface Item {
			itemId: string;
			category?: string;
			region?: string;
		}
		const RegionFacet = new Facet({
			name: 'PATCH_REGION_INPUTS',
			validator: (input: unknown): Item => input as Item,
			PK: { keys: ['itemId'], prefix: '#PRII' },
			SK: { keys: [], prefix: '#PRII' },
			connection: { dynamoDb: ddb, tableName },
		}).addIndex({
			index: Index.GSI1,
			PK: {
				keys: ['category'],
				shard: { count: 4, keys: ['region'] },
				prefix: '#PRIC',
			},
			SK: { keys: [], prefix: '#PRIC' },
		});
		expect(RegionFacet.patchInputs(['category'])).toEqual({
			GSI1PK: ['region'],
		});

		PatchPostFacet.patchInputs(
			// @ts-expect-error pageId composes the PK and is not patchable
			['pageId'],
		);
	});

	test('strict mode enforces key-input completeness at compile time', () => {
		void (async () => {
			// Strict is the default: a bare two-argument call that touches
			// a GSI key input without its co-input does not compile.
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				// @ts-expect-error strict demands authorId for GSI1SK
				{ sendAt: new Date() },
			);

			// Options that carry only a condition stay on the strict
			// overload and keep the check.
			// @ts-expect-error strict demands authorId for GSI1SK
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				{ sendAt: new Date() },
				{ condition: ['postTitle', 'exists'] },
			);

			// Touching a GSI key input without its co-input is rejected;
			// the required property in the error names the missing field.
			// @ts-expect-error strict demands authorId for GSI1SK
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				{ sendAt: new Date() },
				{ missingKeyInputs: 'strict' },
			);

			// The co-input satisfies the demand from either parameter.
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a', authorId: 'a1' },
				{ sendAt: new Date() },
				{ missingKeyInputs: 'strict' },
			);
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				{ sendAt: new Date(), authorId: 'a1' },
				{ missingKeyInputs: 'strict' },
			);

			// Base key fields count as supplied, including as the shard
			// key of GSI1PK (postId) and the co-input of GSI2PK (pageId).
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				{ postStatus: PostStatus.Queued },
				{ missingKeyInputs: 'strict' },
			);

			// A patch that touches no key input has no demands.
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				{ postTitle: 't' },
				{ missingKeyInputs: 'strict' },
			);

			// Non-identity shard keys are demanded like any other input.
			interface Item {
				itemId: string;
				category?: string;
				region?: string;
			}
			const RegionFacet = new Facet({
				name: 'PATCH_REGION',
				validator: (input: unknown): Item => input as Item,
				PK: { keys: ['itemId'], prefix: '#PRI' },
				SK: { keys: [], prefix: '#PRI' },
				connection: { dynamoDb: ddb, tableName },
			}).addIndex({
				index: Index.GSI1,
				PK: {
					keys: ['category'],
					shard: { count: 4, keys: ['region'] },
					prefix: '#PRC',
				},
				SK: { keys: [], prefix: '#PRC' },
			});
			// @ts-expect-error strict demands the shard key region
			await RegionFacet.patch(
				{ itemId: 'x' },
				{ category: 'c' },
				{ missingKeyInputs: 'strict' },
			);
			await RegionFacet.patch(
				{ itemId: 'x', region: 'us' },
				{ category: 'c' },
				{ missingKeyInputs: 'strict' },
			);

			// A patch missing several inputs demands each one, and the
			// demand clears only when every one is supplied.
			interface Wide {
				wideId: string;
				a?: string;
				b?: string;
				c?: string;
			}
			const WideFacet = new Facet({
				name: 'PATCH_WIDE',
				validator: (input: unknown): Wide => input as Wide,
				PK: { keys: ['wideId'], prefix: '#PW' },
				SK: { keys: [], prefix: '#PW' },
				connection: { dynamoDb: ddb, tableName },
			}).addIndex({
				index: Index.GSI3,
				PK: { keys: ['wideId'], prefix: '#PWX' },
				SK: { keys: ['a', 'b', 'c'], prefix: '#PWS' },
			});
			await WideFacet.patch(
				{ wideId: 'w' },
				// @ts-expect-error strict demands both b and c
				{ a: 'x' },
			);
			await WideFacet.patch(
				{ wideId: 'w' },
				// @ts-expect-error strict still demands c
				{ a: 'x', b: 'y' },
			);
			await WideFacet.patch({ wideId: 'w' }, { a: 'x', b: 'y', c: 'z' });
			await WideFacet.patch({ wideId: 'w', c: 'z' }, { a: 'x', b: 'y' });

			// Each missing field surfaces as its own named property.
			type WideDemands = keyof PatchDemands<
				MissingPatchKeyInputs<
					PatchKeyInputGroups<typeof WideFacet>,
					{ a: string },
					{ wideId: string },
					'wideId',
					never
				>
			>;
			const bothDemandsSurface: [WideDemands] extends [
				'Missing key input: b' | 'Missing key input: c',
			]
				? ['Missing key input: b' | 'Missing key input: c'] extends [
						WideDemands,
					]
					? true
					: false
				: false = true;
			void bothDemandsSurface;

			// The demand property is branded: supplying it under its exact
			// name with a plausible value still fails on the unexported
			// brand, so the check cannot be bypassed.
			await PatchPostFacet.patch(
				{ pageId: 'p', postId: 'a' },
				// @ts-expect-error the demand value is an unexported brand
				{ sendAt: new Date(), 'Missing key input: authorId': ['authorId'] },
			);

			// Documented limit, pinned deliberately: a widened patch or
			// query declares every field at the type level, so the
			// compile-time check passes and the runtime backstop takes
			// over instead.
			const widePatch: Partial<Omit<Post, 'pageId' | 'postId'>> = {
				sendAt: new Date(),
			};
			await PatchPostFacet.patch({ pageId: 'p', postId: 'a' }, widePatch, {
				missingKeyInputs: 'strict',
			});
			const wideQuery: Pick<Post, 'pageId' | 'postId'> & Partial<Post> = {
				pageId: 'p',
				postId: 'a',
			};
			await PatchPostFacet.patch(
				wideQuery,
				{ sendAt: new Date() },
				{ missingKeyInputs: 'strict' },
			);
		});

		// The exported groups type carries no undefined from optional
		// facet properties like ttl.
		const groupsHaveNoUndefined: [
			Extract<PatchKeyInputGroups<typeof PatchPostFacet>, undefined>,
		] extends [never]
			? true
			: false = true;
		void groupsHaveNoUndefined;

		// The demand computed for this facet names the missing field: a
		// patch touching only sendAt demands exactly authorId, surfaced
		// as a property named for the field.
		type SendAtDemands = MissingPatchKeyInputs<
			PatchKeyInputGroups<typeof PatchPostFacet>,
			{ sendAt: Date },
			{ pageId: string; postId: string },
			'pageId',
			'postId'
		>;
		const demandsExactlyAuthorId: [SendAtDemands] extends ['authorId']
			? ['authorId'] extends [SendAtDemands]
				? true
				: false
			: false = true;
		void demandsExactlyAuthorId;
		const demandNameSurfaces: [keyof PatchDemands<SendAtDemands>] extends [
			'Missing key input: authorId',
		]
			? true
			: false = true;
		void demandNameSurfaces;

		expect<0>(0 satisfies 0).toBe(0);
	});

	test('a discriminated-union facet keeps variant fields patchable', () => {
		interface Deposit {
			eventId: string;
			kind: 'deposit';
			amount: number;
		}
		interface Withdrawal {
			eventId: string;
			kind: 'withdrawal';
			target: string;
		}
		type LedgerEvent = Deposit | Withdrawal;
		const LedgerFacet = new Facet({
			name: 'PATCH_LEDGER',
			validator: (input: unknown): LedgerEvent => input as LedgerEvent,
			PK: { keys: ['eventId'], prefix: '#PLEDGER' },
			SK: { keys: [], prefix: '#PLEDGER' },
			connection: { dynamoDb: ddb, tableName },
		});

		void (async () => {
			// Variant-specific fields stay patchable. The naive
			// Partial<Omit<LedgerEvent, ...>> would reject both, because
			// Omit collapses a union to its common keys.
			await LedgerFacet.patch({ eventId: 'e' }, { amount: 5 });
			await LedgerFacet.patch({ eventId: 'e' }, { target: 'acct' });
			await LedgerFacet.patch({ eventId: 'e' }, { kind: 'deposit', amount: 5 });

			// Known limit, pinned deliberately: a literal may mix fields
			// from different variants (with or without the discriminant);
			// the validator on the post-patch record is the backstop.
			await LedgerFacet.patch({ eventId: 'e' }, { amount: 5, target: 'x' });
			await LedgerFacet.patch(
				{ eventId: 'e' },
				{ kind: 'deposit', target: 'x' },
			);

			// Wrong value types stay rejected on both variant-specific
			// and shared fields.
			// @ts-expect-error amount must be a number
			await LedgerFacet.patch({ eventId: 'e' }, { amount: 'x' });
			// @ts-expect-error kind must be a variant literal
			await LedgerFacet.patch({ eventId: 'e' }, { kind: 'nope' });

			// @ts-expect-error eventId composes the PK and is not patchable
			await LedgerFacet.patch({ eventId: 'e' }, { eventId: 'other' });
		});

		expect<0>(0 satisfies 0).toBe(0);
	});
});

describe('patchSingleItem expression assembly', () => {
	test('groups actions under one SET and one REMOVE and keeps placeholder namespaces disjoint', async () => {
		const captured: UpdateItemInput[] = [];
		const stub: PatchFacet<Post> = {
			pk: () => '#PPAGE_p1',
			sk: () => '#PPOST_a1',
			out: (record) => record as unknown as Post,
			marshalValue: (value) => toAttributeValue(value),
			ttl: 'deleteAt',
			connection: {
				dynamoDb: {
					updateItem: (input: UpdateItemInput) => {
						captured.push(input);
						const attributes: AttributeMap = { PK: { S: '#PPAGE_p1' } };
						return Promise.resolve({ Attributes: attributes });
					},
				} as unknown as DynamoDB,
				tableName: tableName,
			},
		};
		const targets: PatchKeyTarget<Post>[] = [
			{
				attributeName: 'GSI1SK',
				inputs: ['sendAt', 'authorId'],
				build: () => '#PSEND_rebuilt',
			},
		];

		const result = await patchSingleItem(
			stub,
			targets,
			new Set(['pageId', 'postId']),
			{
				pageId: 'p1',
				postId: 'a1',
				authorId: 'hint-author',
			},
			{
				sendAt: new Date('2026-01-01T00:00:00.000Z'),
				postTitle: undefined,
				deleteAt: 1893456000,
			},
			{ condition: ['postStatus', '=', PostStatus.Queued] },
		);
		expect(result.wasSuccessful).toBe(true);

		const input = captured[0];
		const updateExpression = input.UpdateExpression ?? '';
		// One SET keyword, one REMOVE keyword, in that order.
		expect(updateExpression.match(/SET /g)).toHaveLength(1);
		expect(updateExpression.match(/REMOVE /g)).toHaveLength(1);
		expect(updateExpression).toMatch(/^SET .+ REMOVE .+$/);

		const conditionExpression = input.ConditionExpression ?? '';
		expect(conditionExpression).toContain('attribute_exists (#PK_GUARD)');
		// The hint guard on authorId.
		expect(conditionExpression).toContain('#G_0 = :G_0');
		// The caller condition arrives parenthesized.
		expect(conditionExpression).toMatch(/AND \(.+\)$/);

		const names = input.ExpressionAttributeNames ?? {};
		const values = input.ExpressionAttributeValues ?? {};
		for (const name of Object.keys(names)) {
			expect(name).toMatch(/^#(U_\d+|G_\d+|C_[0-9a-f]+|PK_GUARD)$/);
		}
		for (const value of Object.keys(values)) {
			expect(value).toMatch(/^:(U_\d+|G_\d+|C_[0-9a-f]+)$/);
		}
		expect(names['#G_0']).toBe('authorId');
		expect(values[':G_0']).toEqual({ S: 'hint-author' });
		// The synthetic ttl attribute is set from the normalized value.
		expect(Object.values(names)).toContain('ttl');
		expect(Object.values(values)).toContainEqual({ N: '1893456000' });
		// The recomputed key is set to the builder's output.
		expect(Object.values(values)).toContainEqual({ S: '#PSEND_rebuilt' });

		// Guards never cover an attribute the patch writes, so patch's
		// own conditions stay idempotent under SDK auto-retry.
		const setSection = updateExpression.slice(
			'SET '.length,
			updateExpression.indexOf(' REMOVE '),
		);
		const setAttributes = [...setSection.matchAll(/#U_\d+/g)].map(
			(match) => names[match[0]],
		);
		const guardAttributes = Object.entries(names)
			.filter(([placeholder]) => placeholder.startsWith('#G_'))
			.map(([, attribute]) => attribute);
		expect(setAttributes.length).toBeGreaterThan(0);
		expect(guardAttributes.length).toBeGreaterThan(0);
		expect(
			setAttributes.filter((attribute) => guardAttributes.includes(attribute)),
		).toEqual([]);
	});

	test('a self-sufficient patch carries only the existence condition', async () => {
		const captured: UpdateItemInput[] = [];
		const stub: PatchFacet<Post> = {
			pk: () => '#PPAGE_p1',
			sk: () => '#PPOST_a1',
			out: (record) => record as unknown as Post,
			marshalValue: (value) => toAttributeValue(value),
			connection: {
				dynamoDb: {
					updateItem: (input: UpdateItemInput) => {
						captured.push(input);
						const attributes: AttributeMap = { PK: { S: '#PPAGE_p1' } };
						return Promise.resolve({ Attributes: attributes });
					},
				} as unknown as DynamoDB,
				tableName: tableName,
			},
		};
		const targets: PatchKeyTarget<Post>[] = [
			{
				attributeName: 'GSI1SK',
				inputs: ['sendAt', 'authorId'],
				build: () => '#PSEND_rebuilt',
			},
		];

		const result = await patchSingleItem(
			stub,
			targets,
			new Set(['pageId', 'postId']),
			{ pageId: 'p1', postId: 'a1' },
			{
				sendAt: new Date('2026-01-01T00:00:00.000Z'),
				authorId: 'author-new',
			},
		);
		expect(result.wasSuccessful).toBe(true);

		// Every key input came from the patch or the base identity, so
		// the only condition is the never-upsert existence check and no
		// guard is emitted.
		const input = captured[0];
		expect(input.ConditionExpression).toBe('attribute_exists (#PK_GUARD)');
		const names = Object.keys(input.ExpressionAttributeNames ?? {});
		expect(names.filter((name) => name.startsWith('#G_'))).toHaveLength(0);
	});

	test('read guards cover scalar attribute types and skip document and set attributes', async () => {
		const captured: UpdateItemInput[] = [];
		const stub: PatchFacet<Post> = {
			pk: () => '#PPAGE_p1',
			sk: () => '#PPOST_a1',
			out: () =>
				({
					pageId: 'p1',
					postId: 'a1',
					authorId: 5,
					postTitle: true,
					deleteAt: new Set(['a']),
				}) as unknown as Post,
			marshalValue: (value) => toAttributeValue(value),
			connection: {
				dynamoDb: {
					getItem: () =>
						Promise.resolve({
							Item: {
								PK: { S: '#PPAGE_p1' },
								SK: { S: '#PPOST_a1' },
								authorId: { N: '5' },
								postTitle: { BOOL: true },
								deleteAt: { SS: ['a'] },
							},
						}),
					updateItem: (input: UpdateItemInput) => {
						captured.push(input);
						const attributes: AttributeMap = { PK: { S: '#PPAGE_p1' } };
						return Promise.resolve({ Attributes: attributes });
					},
				} as unknown as DynamoDB,
				tableName: tableName,
			},
		};
		const targets: PatchKeyTarget<Post>[] = [
			{
				attributeName: 'GSI1SK',
				inputs: ['sendAt', 'authorId', 'postTitle', 'deleteAt', 'label'],
				build: () => '#PSEND_rebuilt',
			},
		];

		const result = await patchSingleItem(
			stub,
			targets,
			new Set(['pageId', 'postId']),
			{ pageId: 'p1', postId: 'a1' },
			{ sendAt: new Date('2026-01-01T00:00:00.000Z') },
			{ missingKeyInputs: 'read' },
		);
		expect(result.wasSuccessful).toBe(true);

		const input = captured[0];
		const values = input.ExpressionAttributeValues ?? {};
		const names = input.ExpressionAttributeNames ?? {};
		const guardedAttributes = Object.entries(names)
			.filter(([placeholder]) => placeholder.startsWith('#G_'))
			.map(([, attribute]) => attribute);
		// Number and boolean attributes get equality guards.
		expect(guardedAttributes).toContain('authorId');
		expect(guardedAttributes).toContain('postTitle');
		expect(Object.values(values)).toContainEqual({ N: '5' });
		expect(Object.values(values)).toContainEqual({ BOOL: true });
		// Set attributes never feed a composite key, so no guard.
		expect(guardedAttributes).not.toContain('deleteAt');
		// An input the read shows as absent is asserted absent at write
		// time.
		expect(guardedAttributes).toContain('label');
		expect(input.ConditionExpression ?? '').toMatch(
			/attribute_not_exists \(#G_\d+\)/,
		);
	});

	test('a query hint that cannot feed a composite key is not guarded', async () => {
		const captured: UpdateItemInput[] = [];
		const stub: PatchFacet<Post> = {
			pk: () => '#PPAGE_p1',
			sk: () => '#PPOST_a1',
			out: (record) => record as unknown as Post,
			marshalValue: (value) => toAttributeValue(value),
			connection: {
				dynamoDb: {
					updateItem: (input: UpdateItemInput) => {
						captured.push(input);
						const attributes: AttributeMap = { PK: { S: '#PPAGE_p1' } };
						return Promise.resolve({ Attributes: attributes });
					},
				} as unknown as DynamoDB,
				tableName: tableName,
			},
		};
		const targets: PatchKeyTarget<Post>[] = [
			{
				attributeName: 'GSI1SK',
				inputs: ['sendAt', 'authorId', 'viewCount', 'pinned', 'serial'],
				build: () => '#PSEND_rebuilt',
			},
		];

		const result = await patchSingleItem(
			stub,
			targets,
			new Set(['pageId', 'postId']),
			{
				pageId: 'p1',
				postId: 'a1',
				authorId: new Set(['not-a-key-value']),
				viewCount: 42,
				pinned: true,
				serial: 7n,
			} as unknown as Partial<Post>,
			{ sendAt: new Date('2026-01-01T00:00:00.000Z') },
		);
		expect(result.wasSuccessful).toBe(true);

		const input = captured[0];
		const names = input.ExpressionAttributeNames ?? {};
		const values = input.ExpressionAttributeValues ?? {};
		const guardedAttributes = Object.entries(names)
			.filter(([placeholder]) => placeholder.startsWith('#G_'))
			.map(([, attribute]) => attribute);
		// A Set can never appear in a composite key, so no guard.
		expect(guardedAttributes).not.toContain('authorId');
		// Number, boolean, and bigint hints can all feed a key.
		expect(guardedAttributes).toContain('viewCount');
		expect(guardedAttributes).toContain('pinned');
		expect(guardedAttributes).toContain('serial');
		expect(Object.values(values)).toContainEqual({ N: '42' });
		expect(Object.values(values)).toContainEqual({ BOOL: true });
	});
});

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
