import { DynamoDB, ResourceInUseException } from '@aws-sdk/client-dynamodb';
import {
	BaseCollectionQuery,
	collection,
	FacetCollection,
	OrderedCollectionQuery,
	OrderedFacetCollection,
	single,
	type Grouped,
	type TaggedItem,
} from './collection.js';
import { Facet } from './facet.js';
import { crcShard } from './hash/crc-shard.js';
import { Index } from './keys.js';
import { wait } from './wait.js';

const ddb = new DynamoDB({
	region: 'us-east-1',
	endpoint: 'http://localhost:8000',
});

const tableName = 'TEST';
const connection = { dynamoDb: ddb, tableName };

/**
 * Real validators, unlike the pass-through casts elsewhere in the
 * suite: collection routing depends on validators accepting their own
 * facet's rows and rejecting foreign or malformed ones.
 */
function requireString(record: Record<string, unknown>, field: string): string {
	const value = record[field];
	if (typeof value !== 'string') {
		throw new Error(`Expected "${field}" to be a string`);
	}
	return value;
}

function requireDate(record: Record<string, unknown>, field: string): Date {
	const value = record[field];
	if (typeof value !== 'string' && !(value instanceof Date)) {
		throw new Error(`Expected "${field}" to be a date`);
	}
	const date = new Date(value);
	if (Number.isNaN(date.getTime())) {
		throw new Error(`Expected "${field}" to be a valid date`);
	}
	return date;
}

/**
 * The screen-load domain: four entity types deliberately sharing the
 * `PAGE_<pageId>` partition, with unrelated sort-key layouts.
 */
interface Page {
	pageId: string;
	pageName: string;
}
interface Settings {
	pageId: string;
	theme: string;
}
interface Post {
	pageId: string;
	postId: string;
	postTitle: string;
	createdAt: Date;
}
interface Subscriber {
	pageId: string;
	subscriberId: string;
	email: string;
}
interface Profile {
	pageId: string;
	profileId: string;
	displayName: string;
}
interface Orphan {
	pageId: string;
	orphanId: string;
}
interface Early {
	pageId: string;
	earlyId: string;
}

const PageFacet = new Facet({
	name: 'PAGE',
	validator: (input: unknown): Page => {
		const record = input as Record<string, unknown>;
		return {
			pageId: requireString(record, 'pageId'),
			pageName: requireString(record, 'pageName'),
		};
	},
	PK: { keys: ['pageId'], prefix: 'PAGE' },
	SK: { keys: [], prefix: 'PAGE' },
	connection,
});

const SettingsFacet = new Facet({
	name: 'SETTINGS',
	validator: (input: unknown): Settings => {
		const record = input as Record<string, unknown>;
		return {
			pageId: requireString(record, 'pageId'),
			theme: requireString(record, 'theme'),
		};
	},
	PK: { keys: ['pageId'], prefix: 'PAGE' },
	SK: { keys: [], prefix: 'SETTINGS' },
	connection,
});

const PostFacet = new Facet({
	name: 'POST',
	validator: (input: unknown): Post => {
		const record = input as Record<string, unknown>;
		return {
			pageId: requireString(record, 'pageId'),
			postId: requireString(record, 'postId'),
			postTitle: requireString(record, 'postTitle'),
			createdAt: requireDate(record, 'createdAt'),
		};
	},
	PK: { keys: ['pageId'], prefix: 'PAGE' },
	SK: { keys: ['postId'], prefix: 'POST' },
	connection,
});

const SubscriberFacet = new Facet({
	name: 'SUBSCRIBER',
	validator: (input: unknown): Subscriber => {
		const record = input as Record<string, unknown>;
		return {
			pageId: requireString(record, 'pageId'),
			subscriberId: requireString(record, 'subscriberId'),
			email: requireString(record, 'email'),
		};
	},
	PK: { keys: ['pageId'], prefix: 'PAGE' },
	SK: { keys: ['subscriberId'], prefix: 'SUB' },
	connection,
});

const ProfileFacet = new Facet({
	name: 'PROFILE',
	validator: (input: unknown): Profile => {
		const record = input as Record<string, unknown>;
		return {
			pageId: requireString(record, 'pageId'),
			profileId: requireString(record, 'profileId'),
			displayName: requireString(record, 'displayName'),
		};
	},
	PK: { keys: ['pageId'], prefix: 'PAGE' },
	SK: { keys: ['profileId'], prefix: 'PROFILE' },
	connection,
});

/**
 * Shares the page partition but is deliberately left out of the
 * collections, so its rows exercise the `unmatched` channel.
 */
const OrphanFacet = new Facet({
	name: 'ORPHAN',
	validator: (input: unknown): Orphan => {
		const record = input as Record<string, unknown>;
		return {
			pageId: requireString(record, 'pageId'),
			orphanId: requireString(record, 'orphanId'),
		};
	},
	PK: { keys: ['pageId'], prefix: 'PAGE' },
	SK: { keys: ['orphanId'], prefix: 'ORPHAN' },
	connection,
});

/**
 * Sorts before every member prefix (`AAA` < `PAGE`), so `first()` has
 * to page past its rows.
 */
const EarlyFacet = new Facet({
	name: 'EARLY',
	validator: (input: unknown): Early => {
		const record = input as Record<string, unknown>;
		return {
			pageId: requireString(record, 'pageId'),
			earlyId: requireString(record, 'earlyId'),
		};
	},
	PK: { keys: ['pageId'], prefix: 'PAGE' },
	SK: { keys: ['earlyId'], prefix: 'AAA' },
	connection,
});

const pageScreen = collection({
	page: single(PageFacet),
	settings: single(SettingsFacet),
	post: PostFacet,
	subscriber: SubscriberFacet,
});

const profileScreen = collection({
	profile: single(ProfileFacet),
	post: PostFacet,
});

/**
 * The mixed-timeline domain: three event types sharing the
 * `THREAD_<threadId>` partition on one `EVT`-prefixed sort-key axis
 * with per-member tie-breakers. `Edit` also owns a domain `type`
 * field, which must coexist with the `TaggedItem` wrapper's `type`.
 */
interface Comment {
	threadId: string;
	createdAt: Date;
	commentId: string;
	authorId: string;
	body: string;
}
interface Reaction {
	threadId: string;
	createdAt: Date;
	reactionId: string;
	authorId: string;
	emoji: string;
}
interface Edit {
	threadId: string;
	createdAt: Date;
	authorId: string;
	type: 'title' | 'body';
	diff: string;
}
interface Noise {
	threadId: string;
	noiseId: string;
}

const CommentFacet = new Facet({
	name: 'COMMENT',
	validator: (input: unknown): Comment => {
		const record = input as Record<string, unknown>;
		return {
			threadId: requireString(record, 'threadId'),
			createdAt: requireDate(record, 'createdAt'),
			commentId: requireString(record, 'commentId'),
			authorId: requireString(record, 'authorId'),
			body: requireString(record, 'body'),
		};
	},
	PK: { keys: ['threadId'], prefix: 'THREAD' },
	SK: { keys: ['createdAt', 'commentId'], prefix: 'EVT' },
	connection,
});

const ReactionFacet = new Facet({
	name: 'REACTION',
	validator: (input: unknown): Reaction => {
		const record = input as Record<string, unknown>;
		return {
			threadId: requireString(record, 'threadId'),
			createdAt: requireDate(record, 'createdAt'),
			reactionId: requireString(record, 'reactionId'),
			authorId: requireString(record, 'authorId'),
			emoji: requireString(record, 'emoji'),
		};
	},
	PK: { keys: ['threadId'], prefix: 'THREAD' },
	SK: { keys: ['createdAt', 'reactionId'], prefix: 'EVT' },
	connection,
});

const EditFacet = new Facet({
	name: 'EDIT',
	validator: (input: unknown): Edit => {
		const record = input as Record<string, unknown>;
		const editType = requireString(record, 'type');
		if (editType !== 'title' && editType !== 'body') {
			throw new Error('Expected "type" to be "title" or "body"');
		}
		return {
			threadId: requireString(record, 'threadId'),
			createdAt: requireDate(record, 'createdAt'),
			authorId: requireString(record, 'authorId'),
			type: editType,
			diff: requireString(record, 'diff'),
		};
	},
	PK: { keys: ['threadId'], prefix: 'THREAD' },
	SK: { keys: ['createdAt'], prefix: 'EVT' },
	connection,
});

/**
 * Sorts after every timeline row (`ZZZ` > `EVT`), so a descending
 * `latest()` traversal has to page past its rows before it finds a
 * member record.
 */
const NoiseFacet = new Facet({
	name: 'NOISE',
	validator: (input: unknown): Noise => {
		const record = input as Record<string, unknown>;
		return {
			threadId: requireString(record, 'threadId'),
			noiseId: requireString(record, 'noiseId'),
		};
	},
	PK: { keys: ['threadId'], prefix: 'THREAD' },
	SK: { keys: ['noiseId'], prefix: 'ZZZ' },
	connection,
});

const timelineMembers = {
	comment: CommentFacet,
	reaction: ReactionFacet,
	edit: EditFacet,
};

const activityFeed = collection(timelineMembers, { orderBy: 'createdAt' });

/**
 * The GSI retrofit domain: base keys never shared an axis, so the axis
 * lives on `GSI1` where each member leads its index sort key with
 * `updatedAt`.
 */
interface Task {
	projectId: string;
	taskId: string;
	updatedAt: Date;
	title: string;
}
interface Note {
	projectId: string;
	noteId: string;
	updatedAt: Date;
	text: string;
}

const TaskFacet = new Facet({
	name: 'TASK',
	validator: (input: unknown): Task => {
		const record = input as Record<string, unknown>;
		return {
			projectId: requireString(record, 'projectId'),
			taskId: requireString(record, 'taskId'),
			updatedAt: requireDate(record, 'updatedAt'),
			title: requireString(record, 'title'),
		};
	},
	PK: { keys: ['projectId'], prefix: 'PROJECT' },
	SK: { keys: ['taskId'], prefix: 'TASK' },
	connection,
}).addIndex({
	index: Index.GSI1,
	PK: { keys: ['projectId'], prefix: 'PROJACT' },
	SK: { keys: ['updatedAt', 'taskId'], prefix: 'ACT' },
});

const NoteFacet = new Facet({
	name: 'NOTE',
	validator: (input: unknown): Note => {
		const record = input as Record<string, unknown>;
		return {
			projectId: requireString(record, 'projectId'),
			noteId: requireString(record, 'noteId'),
			updatedAt: requireDate(record, 'updatedAt'),
			text: requireString(record, 'text'),
		};
	},
	PK: { keys: ['projectId'], prefix: 'PROJECT' },
	SK: { keys: ['noteId'], prefix: 'NOTE' },
	connection,
}).addIndex({
	index: Index.GSI1,
	PK: { keys: ['projectId'], prefix: 'PROJACT' },
	SK: { keys: ['updatedAt', 'noteId'], prefix: 'ACT' },
});

const projectActivity = collection(
	{ task: TaskFacet, note: NoteFacet },
	{ orderBy: 'updatedAt', index: Index.GSI1 },
);

/**
 * The variable-width string-axis domain, pinning the documented
 * residual: a string axis orders lexicographically, so unpadded
 * numeric strings sort as text.
 */
interface CounterA {
	scopeId: string;
	seq: string;
	counterId: string;
}
interface CounterB {
	scopeId: string;
	seq: string;
}

const CounterAFacet = new Facet({
	name: 'COUNTER_A',
	validator: (input: unknown): CounterA => {
		const record = input as Record<string, unknown>;
		return {
			scopeId: requireString(record, 'scopeId'),
			seq: requireString(record, 'seq'),
			counterId: requireString(record, 'counterId'),
		};
	},
	PK: { keys: ['scopeId'], prefix: 'SCOPE' },
	SK: { keys: ['seq', 'counterId'], prefix: 'SEQ' },
	connection,
});

const CounterBFacet = new Facet({
	name: 'COUNTER_B',
	validator: (input: unknown): CounterB => {
		const record = input as Record<string, unknown>;
		return {
			scopeId: requireString(record, 'scopeId'),
			seq: requireString(record, 'seq'),
		};
	},
	PK: { keys: ['scopeId'], prefix: 'SCOPE' },
	SK: { keys: ['seq'], prefix: 'SEQ' },
	connection,
});

const seqFeed = collection(
	{ a: CounterAFacet, b: CounterBFacet },
	{ orderBy: 'seq' },
);

/**
 * The sharded domain: both members hash the same shard keys with the
 * same count, so they resolve the same shard for any partition value.
 */
interface Sensor {
	groupId: string;
	sensorId: string;
}
interface Alarm {
	groupId: string;
	alarmId: string;
}

const SensorFacet = new Facet({
	name: 'SENSOR',
	validator: (input: unknown): Sensor => {
		const record = input as Record<string, unknown>;
		return {
			groupId: requireString(record, 'groupId'),
			sensorId: requireString(record, 'sensorId'),
		};
	},
	PK: {
		keys: ['groupId'],
		prefix: 'GROUP',
		shard: { count: 4, keys: ['groupId'] },
	},
	SK: { keys: ['sensorId'], prefix: 'SENSOR' },
	connection,
});

const AlarmFacet = new Facet({
	name: 'ALARM',
	validator: (input: unknown): Alarm => {
		const record = input as Record<string, unknown>;
		return {
			groupId: requireString(record, 'groupId'),
			alarmId: requireString(record, 'alarmId'),
		};
	},
	PK: {
		keys: ['groupId'],
		prefix: 'GROUP',
		shard: { count: 4, keys: ['groupId'] },
	},
	SK: { keys: ['alarmId'], prefix: 'ALARM' },
	connection,
});

const shardedDevices = collection({
	sensor: SensorFacet,
	alarm: AlarmFacet,
});

/**
 * Timeline seeds for `thread-1`, in ascending axis order. Comment and
 * reaction ids are chosen so ties interleave deterministically.
 */
const c1: Comment = {
	threadId: 'thread-1',
	createdAt: new Date('2024-01-05T10:00:00.000Z'),
	commentId: 'a-c1',
	authorId: 'alice',
	body: 'first comment',
};
const r1: Reaction = {
	threadId: 'thread-1',
	createdAt: new Date('2024-01-05T10:00:00.000Z'),
	reactionId: 'b-r1',
	authorId: 'bob',
	emoji: 'tada',
};
const e1: Edit = {
	threadId: 'thread-1',
	createdAt: new Date('2024-01-15T00:00:00.000Z'),
	authorId: 'alice',
	type: 'title',
	diff: 'renamed the thread',
};
const c2: Comment = {
	threadId: 'thread-1',
	createdAt: new Date('2024-02-10T08:00:00.000Z'),
	commentId: 'a-c2',
	authorId: 'bob',
	body: 'second comment',
};
const r2: Reaction = {
	threadId: 'thread-1',
	createdAt: new Date('2024-03-01T12:00:00.000Z'),
	reactionId: 'b-r2',
	authorId: 'alice',
	emoji: 'heart',
};
const e2: Edit = {
	threadId: 'thread-1',
	createdAt: new Date('2024-03-20T09:30:00.000Z'),
	authorId: 'alice',
	type: 'body',
	diff: 'fixed a typo',
};

/** The sort keys of every `thread-1` record, ascending. */
const timelineAscending = [
	'a-c1',
	'b-r1',
	'2024-01-15',
	'a-c2',
	'b-r2',
	'2024-03-20',
];

function timelineKey(item: TaggedItem<typeof timelineMembers>): string {
	switch (item.type) {
		case 'comment':
			return item.record.commentId;
		case 'reaction':
			return item.record.reactionId;
		case 'edit':
			return item.record.createdAt.toISOString().slice(0, 10);
	}
}

describe('collection', () => {
	beforeAll(async () => {
		await createTestTable();

		/**
		 * page-1: the full screen: one page, one settings, three posts,
		 * two subscribers.
		 */
		const pagePuts = await PageFacet.put([
			{ pageId: 'page-1', pageName: 'First Page' },
			{ pageId: 'page-2', pageName: 'Sparse Page' },
			{ pageId: 'page-3', pageName: 'Noisy Page' },
		]);
		expect(pagePuts.hasFailures).toBe(false);

		const settingsPut = await SettingsFacet.put({
			pageId: 'page-1',
			theme: 'dark',
		});
		expect(settingsPut.wasSuccessful).toBe(true);

		const postPuts = await PostFacet.put([
			{
				pageId: 'page-1',
				postId: 'p1',
				postTitle: 'Hello',
				createdAt: new Date('2024-01-03T00:00:00.000Z'),
			},
			{
				pageId: 'page-1',
				postId: 'p2',
				postTitle: 'World',
				createdAt: new Date('2024-01-05T00:00:00.000Z'),
			},
			{
				pageId: 'page-1',
				postId: 'p3',
				postTitle: 'Again',
				createdAt: new Date('2024-01-07T00:00:00.000Z'),
			},
			{
				pageId: 'page-first',
				postId: 'p9',
				postTitle: 'Behind the noise',
				createdAt: new Date('2024-02-01T00:00:00.000Z'),
			},
			{
				pageId: 'page-arity',
				postId: 'p10',
				postTitle: 'Arity partition post',
				createdAt: new Date('2024-02-02T00:00:00.000Z'),
			},
			{
				pageId: 'page-invalid',
				postId: 'z-good',
				postTitle: 'The valid post',
				createdAt: new Date('2024-02-03T00:00:00.000Z'),
			},
		]);
		expect(postPuts.hasFailures).toBe(false);

		const subscriberPuts = await SubscriberFacet.put([
			{ pageId: 'page-1', subscriberId: 's1', email: 'one@example.com' },
			{ pageId: 'page-1', subscriberId: 's2', email: 'two@example.com' },
		]);
		expect(subscriberPuts.hasFailures).toBe(false);

		/**
		 * page-3: an orphan facet's row plus a legacy row written before
		 * the `facet` attribute existed.
		 */
		const orphanPut = await OrphanFacet.put({
			pageId: 'page-3',
			orphanId: 'o1',
		});
		expect(orphanPut.wasSuccessful).toBe(true);
		await ddb.putItem({
			TableName: tableName,
			Item: {
				PK: { S: 'PAGE_page-3' },
				SK: { S: 'LEGACY_1' },
				pageId: { S: 'page-3' },
			},
		});

		/**
		 * page-first: rows that sort before every member prefix, so
		 * `first()` has to page past them.
		 */
		const earlyPuts = await EarlyFacet.put([
			{ pageId: 'page-first', earlyId: 'e1' },
			{ pageId: 'page-first', earlyId: 'e2' },
			{ pageId: 'page-first', earlyId: 'e3' },
		]);
		expect(earlyPuts.hasFailures).toBe(false);

		/**
		 * page-arity: two rows for a member declared single().
		 */
		const profilePuts = await ProfileFacet.put([
			{ pageId: 'page-arity', profileId: 'a-first', displayName: 'Kept' },
			{ pageId: 'page-arity', profileId: 'b-second', displayName: 'Extra' },
		]);
		expect(profilePuts.hasFailures).toBe(false);

		/**
		 * page-invalid: a post row that fails the post validator, sorting
		 * before the valid post (`POST_a-bad` < `POST_z-good`).
		 */
		const invalidPost = {
			pageId: 'page-invalid',
			postId: 'a-bad',
			createdAt: new Date('2024-02-03T00:00:00.000Z'),
		} as Post;
		// A single put writes first and validates the read-back after, so
		// the malformed row persists while the put reports the failure.
		const invalidPut = await PostFacet.put(invalidPost);
		expect(invalidPut.wasSuccessful).toBe(false);

		/**
		 * thread-1: the mixed timeline.
		 */
		const commentPuts = await CommentFacet.put([c1, c2]);
		expect(commentPuts.hasFailures).toBe(false);
		const reactionPuts = await ReactionFacet.put([r1, r2]);
		expect(reactionPuts.hasFailures).toBe(false);
		const editPuts = await EditFacet.put([e1, e2]);
		expect(editPuts.hasFailures).toBe(false);

		/**
		 * thread-noise: two member records buried behind rows that sort
		 * after them, so descending traversals must page.
		 */
		const noisyEventPuts = await CommentFacet.put([
			{
				threadId: 'thread-noise',
				createdAt: new Date('2024-04-01T00:00:00.000Z'),
				commentId: 'nc1',
				authorId: 'alice',
				body: 'buried comment',
			},
		]);
		expect(noisyEventPuts.hasFailures).toBe(false);
		const noisyEditPuts = await EditFacet.put([
			{
				threadId: 'thread-noise',
				createdAt: new Date('2024-04-02T00:00:00.000Z'),
				authorId: 'bob',
				type: 'body',
				diff: 'buried edit',
			},
		]);
		expect(noisyEditPuts.hasFailures).toBe(false);
		const noisePuts = await NoiseFacet.put([
			{ threadId: 'thread-noise', noiseId: 'n1' },
			{ threadId: 'thread-noise', noiseId: 'n2' },
			{ threadId: 'thread-noise', noiseId: 'n3' },
		]);
		expect(noisePuts.hasFailures).toBe(false);

		/**
		 * proj-1: the GSI timeline.
		 */
		const taskPuts = await TaskFacet.put([
			{
				projectId: 'proj-1',
				taskId: 't1',
				updatedAt: new Date('2024-01-10T00:00:00.000Z'),
				title: 'Design',
			},
			{
				projectId: 'proj-1',
				taskId: 't2',
				updatedAt: new Date('2024-03-10T00:00:00.000Z'),
				title: 'Ship',
			},
		]);
		expect(taskPuts.hasFailures).toBe(false);
		const notePuts = await NoteFacet.put([
			{
				projectId: 'proj-1',
				noteId: 'n1',
				updatedAt: new Date('2024-02-10T00:00:00.000Z'),
				text: 'Meeting notes',
			},
		]);
		expect(notePuts.hasFailures).toBe(false);

		/**
		 * scope-1: unpadded numeric strings on a string axis.
		 */
		const counterAPuts = await CounterAFacet.put([
			{ scopeId: 'scope-1', seq: '9', counterId: 'x' },
		]);
		expect(counterAPuts.hasFailures).toBe(false);
		const counterBPuts = await CounterBFacet.put([
			{ scopeId: 'scope-1', seq: '10' },
			{ scopeId: 'scope-1', seq: '100' },
		]);
		expect(counterBPuts.hasFailures).toBe(false);

		/**
		 * group-1: a sharded shared partition.
		 */
		const sensorPuts = await SensorFacet.put([
			{ groupId: 'group-1', sensorId: 'sen-1' },
			{ groupId: 'group-1', sensorId: 'sen-2' },
		]);
		expect(sensorPuts.hasFailures).toBe(false);
		const alarmPuts = await AlarmFacet.put([
			{ groupId: 'group-1', alarmId: 'al-1' },
		]);
		expect(alarmPuts.hasFailures).toBe(false);
	}, 60_000);

	describe('screen load (default kind)', () => {
		test('listAll returns the whole aggregate in one call', async () => {
			const screen = await pageScreen.query({ pageId: 'page-1' }).listAll();

			expect(screen.grouped.page?.pageName).toBe('First Page');
			expect(screen.grouped.settings?.theme).toBe('dark');
			expect(screen.grouped.post.map((post) => post.postId)).toEqual([
				'p1',
				'p2',
				'p3',
			]);
			expect(screen.grouped.subscriber.map((sub) => sub.email)).toEqual([
				'one@example.com',
				'two@example.com',
			]);
			expect(screen.records).toHaveLength(7);
			expect(screen.cursor).toBeUndefined();
			expect(screen.unmatched).toEqual([]);
			expect(screen.failed).toEqual([]);
		});

		test('records interleave in sort-key order with member tags', async () => {
			const screen = await pageScreen.query({ pageId: 'page-1' }).listAll();

			// SK order: PAGE < POST_p1..p3 < SETTINGS < SUB_s1..s2.
			expect(screen.records.map((item) => item.type)).toEqual([
				'page',
				'post',
				'post',
				'post',
				'settings',
				'subscriber',
				'subscriber',
			]);
		});

		test('grouped and records views are consistent', async () => {
			const screen = await pageScreen.query({ pageId: 'page-1' }).listAll();

			const groupedCount =
				(screen.grouped.page ? 1 : 0) +
				(screen.grouped.settings ? 1 : 0) +
				screen.grouped.post.length +
				screen.grouped.subscriber.length;
			expect(groupedCount).toBe(screen.records.length);
			expect(screen.grouped.post[0]).toBe(
				screen.records.find((item) => item.type === 'post')?.record,
			);
		});

		test('validated records carry real model types', async () => {
			const screen = await pageScreen.query({ pageId: 'page-1' }).listAll();

			const post = screen.grouped.post[0];
			expect(post.createdAt).toBeInstanceOf(Date);
			expect(post.createdAt.toISOString()).toBe('2024-01-03T00:00:00.000Z');
			expect(post).not.toHaveProperty('facet');
			expect(post).not.toHaveProperty('PK');
			expect(post).not.toHaveProperty('SK');
		});

		test('a partition missing member types reads as undefined and []', async () => {
			const screen = await pageScreen.query({ pageId: 'page-2' }).listAll();

			expect(screen.grouped.page?.pageName).toBe('Sparse Page');
			expect(screen.grouped.settings).toBeUndefined();
			expect(screen.grouped.post).toEqual([]);
			expect(screen.grouped.subscriber).toEqual([]);
		});

		test('an empty partition returns empty channels and no cursor', async () => {
			const screen = await pageScreen.query({ pageId: 'page-none' }).listAll();

			expect(screen.records).toEqual([]);
			expect(screen.grouped.page).toBeUndefined();
			expect(screen.grouped.post).toEqual([]);
			expect(screen.cursor).toBeUndefined();
		});

		test('consistentRead is accepted on base-table queries', async () => {
			const screen = await pageScreen
				.query({ pageId: 'page-1' })
				.listAll({ consistentRead: true });

			expect(screen.records).toHaveLength(7);
		});
	});

	describe('pagination', () => {
		test('paged list() reflects the current page only and pages by cursor', async () => {
			const firstPage = await pageScreen
				.query({ pageId: 'page-1' })
				.list({ limit: 3 });

			expect(firstPage.records).toHaveLength(3);
			expect(firstPage.cursor).toBeDefined();
			// Page one holds the page record and the first two posts; the
			// per-page grouped view reflects this page only, and must not
			// pretend the rest of the partition is absent.
			expect(firstPage.grouped.page?.pageName).toBe('First Page');
			expect(firstPage.grouped.settings).toBeUndefined();
			expect(firstPage.grouped.post.map((post) => post.postId)).toEqual([
				'p1',
				'p2',
			]);
			expect(firstPage.grouped.subscriber).toEqual([]);

			const collected = [...firstPage.records];
			let cursor = firstPage.cursor;
			while (cursor) {
				const page = await pageScreen
					.query({ pageId: 'page-1' })
					.list({ limit: 3, cursor });
				collected.push(...page.records);
				cursor = page.cursor;
			}

			const drained = await pageScreen.query({ pageId: 'page-1' }).listAll();
			expect(collected).toEqual(drained.records);
		});

		test('cursors round-trip on ordered queries', async () => {
			const collected: TaggedItem<typeof timelineMembers>[] = [];
			let cursor: string | undefined;
			do {
				const page = await activityFeed
					.query({ threadId: 'thread-1' })
					.list({ limit: 2, cursor });
				collected.push(...page.records);
				cursor = page.cursor;
			} while (cursor);

			expect(collected.map(timelineKey)).toEqual(timelineAscending);
		});
	});

	describe('routing and error handling', () => {
		test('unknown-facet and legacy rows land in unmatched, never dropped', async () => {
			const screen = await pageScreen.query({ pageId: 'page-3' }).listAll();

			expect(screen.grouped.page?.pageName).toBe('Noisy Page');
			expect(screen.unmatched).toHaveLength(2);
			const unmatchedSks = screen.unmatched
				.map((item) => item.SK.S ?? '')
				.sort();
			expect(unmatchedSks).toEqual(['LEGACY_1', 'ORPHAN_o1']);
			expect(screen.failed).toEqual([]);
		});

		test('onUnknown: "throw" rejects on a row with no facet attribute', async () => {
			// The legacy row sorts first in page-3 (LEGACY_1 < ORPHAN_o1).
			await expect(
				pageScreen.query({ pageId: 'page-3' }).listAll({ onUnknown: 'throw' }),
			).rejects.toThrow(/no facet attribute/);
		});

		test('onUnknown: "throw" names the foreign facet', async () => {
			// page-arity holds PROFILE rows, which pageScreen does not know.
			await expect(
				pageScreen
					.query({ pageId: 'page-arity' })
					.listAll({ onUnknown: 'throw' }),
			).rejects.toThrow(/facet "PROFILE" matches no member/);
		});

		test('an invalid row throws by default', async () => {
			await expect(
				pageScreen.query({ pageId: 'page-invalid' }).listAll(),
			).rejects.toThrow('Expected "postTitle" to be a string');
		});

		test('onInvalid: "collect" keeps the page alive and reports the row', async () => {
			const screen = await pageScreen
				.query({ pageId: 'page-invalid' })
				.listAll({ onInvalid: 'collect' });

			expect(screen.grouped.post.map((post) => post.postId)).toEqual([
				'z-good',
			]);
			expect(screen.failed).toHaveLength(1);
			expect(screen.failed[0].facet).toBe('post');
			expect(screen.failed[0].error).toBeInstanceOf(Error);
			expect(screen.failed[0].item.SK.S).toBe('POST_a-bad');
			expect(screen.unmatched).toEqual([]);
		});

		test('a second row for a single() member keeps the first and reports the extra', async () => {
			const screen = await profileScreen
				.query({ pageId: 'page-arity' })
				.listAll();

			expect(screen.grouped.profile?.displayName).toBe('Kept');
			expect(screen.failed).toHaveLength(1);
			expect(screen.failed[0].facet).toBe('profile');
			expect(String(screen.failed[0].error)).toMatch(/single\(\)/);
			// The extra row does not leak into the interleaved view either.
			expect(
				screen.records.filter((item) => item.type === 'profile'),
			).toHaveLength(1);
			expect(screen.grouped.post.map((post) => post.postId)).toEqual(['p10']);
		});
	});

	describe('first()', () => {
		test('returns the first member record in sort-key order', async () => {
			const item = await pageScreen.query({ pageId: 'page-1' }).first();

			expect(item?.type).toBe('page');
			if (item?.type === 'page') {
				expect(item.record.pageName).toBe('First Page');
			}
		});

		test('pages past leading unmatched rows instead of returning a false null', async () => {
			const item = await pageScreen.query({ pageId: 'page-first' }).first();

			expect(item?.type).toBe('post');
			if (item?.type === 'post') {
				expect(item.record.postId).toBe('p9');
			}
		});

		test('pages past a leading invalid row with onInvalid: "collect"', async () => {
			const item = await pageScreen
				.query({ pageId: 'page-invalid' })
				.first({ onInvalid: 'collect' });

			expect(item?.type).toBe('post');
			if (item?.type === 'post') {
				expect(item.record.postId).toBe('z-good');
			}
		});

		test('a leading invalid row throws by default', async () => {
			await expect(
				pageScreen.query({ pageId: 'page-invalid' }).first(),
			).rejects.toThrow('Expected "postTitle" to be a string');
		});

		test('returns null on an empty partition', async () => {
			const item = await pageScreen.query({ pageId: 'page-none' }).first();
			expect(item).toBeNull();
		});
	});

	describe('ordered timeline (base table)', () => {
		test('between takes plain Date values and returns the interleaved window', async () => {
			const window = await activityFeed
				.query({ threadId: 'thread-1' })
				.between(new Date('2024-01-01'), new Date('2024-02-01'));

			expect(window.records.map(timelineKey)).toEqual([
				'a-c1',
				'b-r1',
				'2024-01-15',
			]);
			expect(window.records.map((item) => item.type)).toEqual([
				'comment',
				'reaction',
				'edit',
			]);
		});

		test('between with plain values matches between with sk()-built bounds', async () => {
			const plain = await activityFeed
				.query({ threadId: 'thread-1' })
				.between(new Date('2024-01-01'), new Date('2024-02-01'));
			const composed = await activityFeed
				.query({ threadId: 'thread-1' })
				.between(
					CommentFacet.sk({ createdAt: new Date('2024-01-01') }),
					CommentFacet.sk({ createdAt: new Date('2024-02-01') }),
				);

			expect(plain.records).toEqual(composed.records);
		});

		test('raw-string bounds pass through untouched', async () => {
			const window = await activityFeed
				.query({ threadId: 'thread-1' })
				.between('EVT_2024-01', 'EVT_2024-02');

			expect(window.records.map(timelineKey)).toEqual([
				'a-c1',
				'b-r1',
				'2024-01-15',
			]);
		});

		test('a non-key string composes onto the shared prefix', async () => {
			const window = await activityFeed
				.query({ threadId: 'thread-1' })
				.greaterThanOrEqual('2024-03');

			expect(window.records.map(timelineKey)).toEqual(['b-r2', '2024-03-20']);
		});

		test('comparison operators bound the axis', async () => {
			const after = await activityFeed
				.query({ threadId: 'thread-1' })
				.greaterThanOrEqual(new Date('2024-02-01'));
			expect(after.records.map(timelineKey)).toEqual([
				'a-c2',
				'b-r2',
				'2024-03-20',
			]);

			const before = await activityFeed
				.query({ threadId: 'thread-1' })
				.lessThan(new Date('2024-01-15T00:00:00.000Z'));
			expect(before.records.map(timelineKey)).toEqual(['a-c1', 'b-r1']);

			const strictlyAfter = await activityFeed
				.query({ threadId: 'thread-1' })
				.greaterThan(new Date('2024-01-15T00:00:00.000Z'));
			expect(strictlyAfter.records.map(timelineKey)).toEqual([
				'a-c2',
				'b-r2',
				'2024-03-20',
			]);

			const upTo = await activityFeed
				.query({ threadId: 'thread-1' })
				.lessThanOrEqual(new Date('2024-01-15T00:00:00.000Z'));
			expect(upTo.records.map(timelineKey)).toEqual([
				'a-c1',
				'b-r1',
				'2024-01-15',
			]);
		});

		test('equals matches the full composed sort key only', async () => {
			// The edit's sort key is exactly EVT_<iso>, so it matches.
			const edits = await activityFeed
				.query({ threadId: 'thread-1' })
				.equals(new Date('2024-01-15T00:00:00.000Z'));
			expect(edits.records.map((item) => item.type)).toEqual(['edit']);

			// Comment and reaction rows at this instant carry tie-breaker
			// fields after the axis, so the composed bound matches nothing.
			// This pins the documented equals semantics.
			const ties = await activityFeed
				.query({ threadId: 'thread-1' })
				.equals(new Date('2024-01-05T10:00:00.000Z'));
			expect(ties.records).toEqual([]);
		});

		test('latest(n) returns the n newest records, newest first, without a cursor', async () => {
			const recent = await activityFeed
				.query({ threadId: 'thread-1' })
				.latest(2);

			expect(recent.records.map(timelineKey)).toEqual(['2024-03-20', 'b-r2']);
			expect(recent.cursor).toBeUndefined();
		});

		test('earliest(n) returns the n oldest records, oldest first', async () => {
			const oldest = await activityFeed
				.query({ threadId: 'thread-1' })
				.earliest(3);

			expect(oldest.records.map(timelineKey)).toEqual([
				'a-c1',
				'b-r1',
				'2024-01-15',
			]);
			expect(oldest.cursor).toBeUndefined();
		});

		test('latest(n) larger than the partition returns everything', async () => {
			const everything = await activityFeed
				.query({ threadId: 'thread-1' })
				.latest(50);

			expect(everything.records.map(timelineKey)).toEqual(
				[...timelineAscending].reverse(),
			);
		});

		test('latest(n) pages past trailing foreign rows to fill its count', async () => {
			const recent = await activityFeed
				.query({ threadId: 'thread-noise' })
				.latest(2);

			expect(recent.records.map((item) => item.type)).toEqual([
				'edit',
				'comment',
			]);
			expect(recent.unmatched.length).toBeGreaterThanOrEqual(3);
		});

		test('latest rejects a non-positive count', async () => {
			await expect(
				activityFeed.query({ threadId: 'thread-1' }).latest(0),
			).rejects.toThrow(/positive integer/);
		});

		test('direction flips list()', async () => {
			const ascending = await activityFeed
				.query({ threadId: 'thread-1' })
				.list();
			const descending = await activityFeed
				.query({ threadId: 'thread-1' })
				.list({ direction: 'descending' });

			expect(ascending.records.map(timelineKey)).toEqual(timelineAscending);
			expect(descending.records.map(timelineKey)).toEqual(
				[...timelineAscending].reverse(),
			);
		});

		test('first() follows direction on the ordered kind', async () => {
			const earliest = await activityFeed
				.query({ threadId: 'thread-1' })
				.first();
			expect(earliest && timelineKey(earliest)).toBe('a-c1');

			const newest = await activityFeed
				.query({ threadId: 'thread-1' })
				.first({ direction: 'descending' });
			expect(newest && timelineKey(newest)).toBe('2024-03-20');
		});

		test('filter narrows across member types on a shared field', async () => {
			const byAlice = await activityFeed
				.query({ threadId: 'thread-1' })
				.listAll({ filter: ['authorId', '=', 'alice'] });

			expect(byAlice.records.map(timelineKey)).toEqual([
				'a-c1',
				'2024-01-15',
				'b-r2',
				'2024-03-20',
			]);
		});

		test('a filtered page can be short while a cursor remains', async () => {
			// limit: 2 evaluates c1 (alice) and r1 (bob); the filter then
			// discards r1, so the page holds one record and a cursor.
			const page = await activityFeed
				.query({ threadId: 'thread-1' })
				.list({ filter: ['authorId', '=', 'alice'], limit: 2 });

			expect(page.records.map(timelineKey)).toEqual(['a-c1']);
			expect(page.cursor).toBeDefined();
		});

		test('a member model owning a domain type field works end to end', async () => {
			const window = await activityFeed
				.query({ threadId: 'thread-1' })
				.equals(new Date('2024-01-15T00:00:00.000Z'));

			const item = window.records[0];
			expect(item.type).toBe('edit');
			if (item.type === 'edit') {
				// The wrapper's discriminant and the model's own domain
				// `type` field coexist.
				expect(item.record.type).toBe('title');
			}
		});
	});

	describe('GSI collections', () => {
		test('an ordered GSI query returns the interleaved feed', async () => {
			const feed = await projectActivity
				.query({ projectId: 'proj-1' })
				.listAll();

			expect(feed.records.map((item) => item.type)).toEqual([
				'task',
				'note',
				'task',
			]);
			expect(feed.grouped.task.map((task) => task.taskId)).toEqual([
				't1',
				't2',
			]);
			expect(feed.grouped.note.map((note) => note.noteId)).toEqual(['n1']);
		});

		test('range operators work against the index axis', async () => {
			const window = await projectActivity
				.query({ projectId: 'proj-1' })
				.between(new Date('2024-02-01'), new Date('2024-04-01'));

			expect(window.records.map((item) => item.type)).toEqual(['note', 'task']);

			const newest = await projectActivity
				.query({ projectId: 'proj-1' })
				.latest(1);
			expect(newest.records[0]?.type).toBe('task');
			if (newest.records[0]?.type === 'task') {
				expect(newest.records[0].record.taskId).toBe('t2');
			}
		});

		test('records read from a GSI strip the index keys', async () => {
			const feed = await projectActivity
				.query({ projectId: 'proj-1' })
				.listAll();

			expect(feed.grouped.task[0]).not.toHaveProperty('GSI1PK');
			expect(feed.grouped.task[0]).not.toHaveProperty('GSI1SK');
		});

		test('consistentRead is rejected on a GSI collection query', async () => {
			// The compile-time gate catches the literal; the runtime guard
			// still backstops widened types.
			await expect(
				projectActivity
					.query({ projectId: 'proj-1' })
					// @ts-expect-error consistentRead is gated off on GSI collections
					.list({ consistentRead: true }),
			).rejects.toThrow(/Consistent reads are not supported/);

			await expect(
				projectActivity
					.query({ projectId: 'proj-1' })
					// @ts-expect-error consistentRead is gated off on GSI collections
					.latest(1, { consistentRead: true }),
			).rejects.toThrow(/Consistent reads are not supported/);
		});

		test('construction throws when a member has not registered the index', () => {
			const NoIndexFacet = new Facet({
				name: 'NO_INDEX',
				validator: (input: unknown): Note => input as Note,
				PK: { keys: ['projectId'], prefix: 'PROJECT' },
				SK: { keys: ['updatedAt'], prefix: 'ACT' },
				connection,
			});

			expect(() =>
				collection(
					{ task: TaskFacet, other: NoIndexFacet },
					{ orderBy: 'updatedAt', index: Index.GSI1 },
				),
			).toThrow(/no GSI1 index registered/);
		});

		test('construction throws when a member index SK does not lead with the axis', () => {
			const WrongLeadFacet = new Facet({
				name: 'WRONG_GSI_LEAD',
				validator: (input: unknown): Task => input as Task,
				PK: { keys: ['projectId'], prefix: 'PROJECT' },
				SK: { keys: ['taskId'], prefix: 'WRONGTASK' },
				connection,
			}).addIndex({
				index: Index.GSI1,
				PK: { keys: ['projectId'], prefix: 'PROJACT' },
				SK: { keys: ['title', 'updatedAt'], prefix: 'ACT' },
			});

			expect(() =>
				collection(
					{ task: TaskFacet, wrong: WrongLeadFacet },
					{ orderBy: 'updatedAt', index: Index.GSI1 },
				),
			).toThrow(/GSI1 sort key leads with "title"/);
		});
	});

	describe('ordered construction guards (base table)', () => {
		test('accepts members whose trailing tie-breaker fields differ', () => {
			// comment: [createdAt, commentId]; edit: [createdAt]. Already
			// proven by activityFeed; pinned here as an explicit check.
			expect(() =>
				collection(
					{ comment: CommentFacet, edit: EditFacet },
					{ orderBy: 'createdAt' },
				),
			).not.toThrow();
		});

		test('throws when a member does not lead its SK with the axis', () => {
			const WrongLeadFacet = new Facet({
				name: 'WRONG_LEAD',
				validator: (input: unknown): Comment => input as Comment,
				PK: { keys: ['threadId'], prefix: 'THREAD' },
				SK: { keys: ['commentId', 'createdAt'], prefix: 'EVT' },
				connection,
			});

			expect(() =>
				collection(
					{ comment: CommentFacet, wrong: WrongLeadFacet },
					{ orderBy: 'createdAt' },
				),
			).toThrow(/"wrong".*leads with "commentId"/);
		});

		test('throws when a member has an empty sort key', () => {
			expect(() =>
				collection(
					{ comment: CommentFacet, page: single(PageFacet) },
					// @ts-expect-error createdAt is not shared with Page
					{ orderBy: 'createdAt' },
				),
			).toThrow(/has no sort-key fields/);
		});

		test('throws on mismatched prefixes', () => {
			const OtherPrefixFacet = new Facet({
				name: 'OTHER_PREFIX',
				validator: (input: unknown): Edit => input as Edit,
				PK: { keys: ['threadId'], prefix: 'THREAD' },
				SK: { keys: ['createdAt'], prefix: 'HISTORY' },
				connection,
			});

			expect(() =>
				collection(
					{ comment: CommentFacet, other: OtherPrefixFacet },
					{ orderBy: 'createdAt' },
				),
			).toThrow(/different sort key prefixes/);
		});

		test('throws on mismatched delimiters', () => {
			const OtherDelimiterFacet = new Facet({
				name: 'OTHER_DELIMITER',
				validator: (input: unknown): Edit => input as Edit,
				PK: { keys: ['threadId'], prefix: 'THREAD' },
				SK: { keys: ['createdAt'], prefix: 'EVT' },
				delimiter: '#',
				connection,
			});

			expect(() =>
				collection(
					{ comment: CommentFacet, other: OtherDelimiterFacet },
					{ orderBy: 'createdAt' },
				),
			).toThrow(/different key delimiters/);
		});

		test('throws on a sharded sort key', () => {
			const ShardedSkFacet = new Facet({
				name: 'SHARDED_SK',
				validator: (input: unknown): Edit => input as Edit,
				PK: { keys: ['threadId'], prefix: 'THREAD' },
				SK: {
					keys: ['createdAt'],
					prefix: 'EVT',
					shard: { count: 2, keys: ['authorId'] },
				},
				connection,
			});

			expect(() =>
				collection(
					{ comment: CommentFacet, sharded: ShardedSkFacet },
					{ orderBy: 'createdAt' },
				),
			).toThrow(/sharded sort key/);
		});
	});

	describe('construction guards (both kinds)', () => {
		test('throws on an empty member map', () => {
			expect(() => collection({})).toThrow(/at least one member/);
		});

		test('throws when members use different DynamoDB clients', () => {
			const otherDdb = new DynamoDB({
				region: 'us-east-1',
				endpoint: 'http://localhost:8000',
			});
			const OtherClientFacet = new Facet({
				name: 'OTHER_CLIENT',
				validator: (input: unknown): Page => input as Page,
				PK: { keys: ['pageId'], prefix: 'PAGE' },
				SK: { keys: [], prefix: 'OTHER' },
				connection: { dynamoDb: otherDdb, tableName },
			});

			expect(() =>
				collection({ page: single(PageFacet), other: OtherClientFacet }),
			).toThrow(/different DynamoDB client instances/);
		});

		test('throws when members use different tables', () => {
			const OtherTableFacet = new Facet({
				name: 'OTHER_TABLE',
				validator: (input: unknown): Page => input as Page,
				PK: { keys: ['pageId'], prefix: 'PAGE' },
				SK: { keys: [], prefix: 'OTHER' },
				connection: { dynamoDb: ddb, tableName: 'OTHER_TABLE' },
			});

			expect(() =>
				collection({ page: single(PageFacet), other: OtherTableFacet }),
			).toThrow(/different tables/);
		});

		test('throws when two members share a facet name', () => {
			const DuplicateNameFacet = new Facet({
				name: 'PAGE',
				validator: (input: unknown): Settings => input as Settings,
				PK: { keys: ['pageId'], prefix: 'PAGE' },
				SK: { keys: [], prefix: 'DUPLICATE' },
				connection,
			});

			expect(() =>
				collection({
					page: single(PageFacet),
					duplicate: single(DuplicateNameFacet),
				}),
			).toThrow(/share the facet name "PAGE"/);
		});
	});

	describe('partition-key guard (query time)', () => {
		test('throws when a partition-key field is missing', () => {
			// buildKey would silently truncate the key and read the wrong
			// partition, so the guard demands every member's PK fields.
			// The partition input type is Partial, so an empty object
			// compiles; the runtime guard is what catches the omission.
			expect(() => pageScreen.query({})).toThrow(
				/builds its partition key from "pageId"/,
			);
		});

		test('throws when a member needs a field the others do not', () => {
			interface TenantPost {
				pageId: string;
				tenantId: string;
				postId: string;
			}
			const TenantPostFacet = new Facet({
				name: 'TENANT_POST',
				validator: (input: unknown): TenantPost => input as TenantPost,
				PK: { keys: ['pageId', 'tenantId'], prefix: 'PAGE' },
				SK: { keys: ['postId'], prefix: 'TPOST' },
				connection,
			});
			const mixedPartition = collection({
				page: single(PageFacet),
				post: TenantPostFacet,
			});

			// Both members would build 'PAGE_page-1' after buildKey drops
			// the undefined tenantId, so the string comparison alone would
			// pass while TenantPost rows silently never match.
			expect(() => mixedPartition.query({ pageId: 'page-1' })).toThrow(
				/"post" builds its partition key from "tenantId"/,
			);
		});

		test('throws when members build different partition strings', () => {
			const OtherPartitionFacet = new Facet({
				name: 'OTHER_PARTITION',
				validator: (input: unknown): Page => input as Page,
				PK: { keys: ['pageId'], prefix: 'ORG' },
				SK: { keys: [], prefix: 'OTHER' },
				connection,
			});
			const mismatched = collection({
				page: single(PageFacet),
				other: single(OtherPartitionFacet),
			});

			expect(() => mismatched.query({ pageId: 'page-1' })).toThrow(
				/"PAGE_page-1" versus "ORG_page-1"/,
			);
		});
	});

	describe('sharded partitions', () => {
		test('an explicit shard targets one group across member types', async () => {
			const shard = parseInt(crcShard('group-1', 4), 16);
			const devices = await shardedDevices
				.query({ groupId: 'group-1' }, shard)
				.listAll();

			expect(devices.grouped.sensor.map((sensor) => sensor.sensorId)).toEqual([
				'sen-1',
				'sen-2',
			]);
			expect(devices.grouped.alarm.map((alarm) => alarm.alarmId)).toEqual([
				'al-1',
			]);
		});

		test('omitting the shard hashes the partition fields like the write side', async () => {
			const devices = await shardedDevices
				.query({ groupId: 'group-1' })
				.listAll();

			expect(devices.records).toHaveLength(3);
		});
	});

	describe('round-trips', () => {
		test('a record from grouped feeds straight back into put()', async () => {
			const screen = await pageScreen.query({ pageId: 'page-1' }).listAll();

			const postResult = await PostFacet.put(screen.grouped.post[0]);
			expect(postResult.wasSuccessful).toBe(true);

			const page = screen.grouped.page;
			expect(page).toBeDefined();
			if (page) {
				const pageResult = await PageFacet.put(page);
				expect(pageResult.wasSuccessful).toBe(true);
			}
		});

		test('a record from the interleaved view round-trips unchanged', async () => {
			const window = await activityFeed
				.query({ threadId: 'thread-1' })
				.equals(new Date('2024-01-15T00:00:00.000Z'));
			const item = window.records[0];
			expect(item.type).toBe('edit');
			if (item.type !== 'edit') {
				return;
			}

			const result = await EditFacet.put(item.record);
			expect(result.wasSuccessful).toBe(true);

			const reread = await activityFeed
				.query({ threadId: 'thread-1' })
				.equals(new Date('2024-01-15T00:00:00.000Z'));
			expect(reread.records[0]).toEqual(item);
		});
	});

	describe('variable-width string axis (documented residual)', () => {
		test('unpadded numeric strings order lexicographically, not numerically', async () => {
			const ordered = await seqFeed.query({ scopeId: 'scope-1' }).listAll();

			// '10' < '100' < '9' as strings. This pins the documented
			// caveat: a string axis whose content is variable-width does
			// not order numerically.
			const seqs = ordered.records.map((item) => item.record.seq);
			expect(seqs).toEqual(['10', '100', '9']);
		});

		test('a prefix-shaped string axis value is treated as a raw key', async () => {
			// A stored axis value that itself starts with the shared prefix
			// and delimiter is indistinguishable from a raw key string, so
			// the bound passes through verbatim and misses the composed
			// row. This pins the documented AxisBound passthrough rule.
			const put = await CounterBFacet.put({
				scopeId: 'scope-2',
				seq: 'SEQ_5',
			});
			expect(put.wasSuccessful).toBe(true);

			const byValue = await seqFeed
				.query({ scopeId: 'scope-2' })
				.equals('SEQ_5');
			expect(byValue.records).toEqual([]);

			// The row is reachable through the full composed key.
			const byRawKey = await seqFeed
				.query({ scopeId: 'scope-2' })
				.equals('SEQ_SEQ_5');
			expect(byRawKey.records.map((item) => item.record.seq)).toEqual([
				'SEQ_5',
			]);
		});
	});

	describe('defensive response handling', () => {
		/**
		 * A structural stand-in for a facet, like the transaction tests
		 * use, so a client response without `Items` can be exercised.
		 */
		interface StubEvent {
			stubId: string;
			createdAt: Date;
		}
		const emptyResponseClient = {
			query: () => Promise.resolve({ $metadata: {} }),
		} as unknown as Pick<DynamoDB, 'query'>;
		const stubFacet = {
			name: 'STUB',
			delimiter: '_',
			connection: { dynamoDb: emptyResponseClient, tableName: 'STUB_TABLE' },
			pkLayout: {
				prefix: 'STUB',
				keys: [] as const,
				delimiter: '_',
				sharded: false,
			},
			skLayout: {
				prefix: 'EVT',
				keys: ['createdAt'] as const,
				delimiter: '_',
				sharded: false,
			},
			out: (record: unknown): StubEvent => record as StubEvent,
			pk: () => 'STUB_PARTITION',
		};

		test('a response without Items yields an empty result', async () => {
			const stubbed = collection(
				{ event: stubFacet },
				{ orderBy: 'createdAt' },
			);
			const query = stubbed.query({});

			const listed = await query.list();
			expect(listed.records).toEqual([]);
			expect(listed.cursor).toBeUndefined();

			const drained = await query.listAll();
			expect(drained.grouped.event).toEqual([]);

			expect(await query.first()).toBeNull();

			const bounded = await query.latest(3);
			expect(bounded.records).toEqual([]);
		});
	});

	describe('multi-page listAll', () => {
		beforeAll(async () => {
			const bigPagePut = await PageFacet.put({
				pageId: 'page-big',
				pageName: 'Big Page',
			});
			expect(bigPagePut.wasSuccessful).toBe(true);

			// Five ~300 KB posts push the partition past DynamoDB's 1 MB
			// query page, so listAll must actually follow LastEvaluatedKey.
			const bigPosts: Post[] = [];
			for (let index = 0; index < 5; index++) {
				bigPosts.push({
					pageId: 'page-big',
					postId: `big-${index}`,
					postTitle: 'x'.repeat(300_000),
					createdAt: new Date('2024-01-01T00:00:00.000Z'),
				});
			}
			const bigPuts = await PostFacet.put(bigPosts);
			expect(bigPuts.hasFailures).toBe(false);
		}, 60_000);

		test('a single list() page is cut short by the 1 MB limit', async () => {
			const page = await pageScreen.query({ pageId: 'page-big' }).list();

			expect(page.records.length).toBeLessThan(6);
			expect(page.cursor).toBeDefined();
		});

		test('listAll drains every page and merges all channels', async () => {
			const screen = await pageScreen.query({ pageId: 'page-big' }).listAll();

			expect(screen.grouped.page?.pageName).toBe('Big Page');
			expect(screen.grouped.post.map((post) => post.postId)).toEqual([
				'big-0',
				'big-1',
				'big-2',
				'big-3',
				'big-4',
			]);
			expect(screen.cursor).toBeUndefined();
		});
	});
});

describe('collection type surface', () => {
	test('collections and queries have their declared runtime classes', () => {
		expect(pageScreen).toBeInstanceOf(FacetCollection);
		expect(activityFeed).toBeInstanceOf(OrderedFacetCollection);
		expect(pageScreen.query({ pageId: 'x' })).toBeInstanceOf(
			BaseCollectionQuery,
		);
		expect(activityFeed.query({ threadId: 'x' })).toBeInstanceOf(
			OrderedCollectionQuery,
		);
	});

	test('range vocabulary does not exist on the default kind', () => {
		const query = pageScreen.query({ pageId: 'type-surface' });

		// @ts-expect-error between does not exist on a default collection query
		void query.between;
		// @ts-expect-error equals does not exist on a default collection query
		void query.equals;
		// @ts-expect-error greaterThan does not exist on a default collection query
		void query.greaterThan;
		// @ts-expect-error latest does not exist on a default collection query
		void query.latest;
		// @ts-expect-error earliest does not exist on a default collection query
		void query.earliest;

		expect(query).toBeInstanceOf(BaseCollectionQuery);
	});

	test('a default collection cannot be laundered into an ordered one', () => {
		const defaultTimeline = collection(timelineMembers);

		// @ts-expect-error a default collection is not an ordered collection
		const laundered: typeof activityFeed = defaultTimeline;
		void laundered;

		expect(defaultTimeline).toBeInstanceOf(FacetCollection);
	});

	test('orderBy rejects ineligible axis fields', () => {
		// A field missing from a member is not a valid axis; the runtime
		// layout check rejects it too.
		expect(() =>
			collection(
				timelineMembers,
				// @ts-expect-error commentId is not shared by every member
				{ orderBy: 'commentId' },
			),
		).toThrow(/doesn't sort on the "commentId" axis/);

		interface MixedA {
			scopeId: string;
			at: Date;
		}
		interface MixedB {
			scopeId: string;
			at: string;
		}
		const MixedAFacet = new Facet({
			name: 'MIXED_A',
			validator: (input: unknown): MixedA => input as MixedA,
			PK: { keys: ['scopeId'], prefix: 'MIX' },
			SK: { keys: ['at'], prefix: 'MIXAT' },
			connection,
		});
		const MixedBFacet = new Facet({
			name: 'MIXED_B',
			validator: (input: unknown): MixedB => input as MixedB,
			PK: { keys: ['scopeId'], prefix: 'MIX' },
			SK: { keys: ['at'], prefix: 'MIXAT' },
			connection,
		});
		const mixed = collection(
			{ a: MixedAFacet, b: MixedBFacet },
			// @ts-expect-error at is a Date on one member and a string on the other
			{ orderBy: 'at' },
		);
		expect(mixed).toBeInstanceOf(OrderedFacetCollection);

		interface Numbered {
			scopeId: string;
			position: number;
		}
		const NumberedFacet = new Facet({
			name: 'NUMBERED',
			validator: (input: unknown): Numbered => input as Numbered,
			PK: { keys: ['scopeId'], prefix: 'NUM' },
			SK: { keys: ['position'], prefix: 'POS' },
			connection,
		});
		const numbered = collection(
			{ a: NumberedFacet },
			// @ts-expect-error a number field cannot be an axis
			{ orderBy: 'position' },
		);
		expect(numbered).toBeInstanceOf(OrderedFacetCollection);
	});

	test('compile-time-only assertions', () => {
		// Never executed; these exist for the typechecker.
		const _typeAssertions = async () => {
			// Bound values must match the axis type (or be a raw string).
			// @ts-expect-error a number is not a Date-axis bound
			await activityFeed.query({ threadId: 't' }).between(1, 2);
			// @ts-expect-error a number is not a Date-axis bound
			await activityFeed.query({ threadId: 't' }).greaterThan(42);

			// query() rejects unknown fields in an object literal.
			// @ts-expect-error bogus is not a shared member field
			pageScreen.query({ pageId: 'p', bogus: true });

			// direction is not part of the default kind's vocabulary.
			// @ts-expect-error direction does not exist on default-kind queries
			await pageScreen.query({ pageId: 'p' }).list({ direction: 'ascending' });
			// @ts-expect-error direction does not exist on default-kind first()
			await pageScreen.query({ pageId: 'p' }).first({ direction: 'ascending' });

			// latest()/earliest() manage their own paging and direction.
			await activityFeed
				.query({ threadId: 't' })
				// @ts-expect-error latest does not take a cursor
				.latest(5, { cursor: 'abc' });
			await activityFeed
				.query({ threadId: 't' })
				// @ts-expect-error earliest does not take a direction
				.earliest(5, { direction: 'descending' });

			// listAll() drains the partition, so paging options are gone.
			// @ts-expect-error listAll does not take a limit
			await pageScreen.query({ pageId: 'p' }).listAll({ limit: 5 });

			// grouped refines single() members to Model | undefined.
			const screen = await pageScreen.query({ pageId: 'p' }).listAll();
			// @ts-expect-error a single() member is not an array
			void screen.grouped.page[0];
			// @ts-expect-error a single() member may be undefined
			void screen.grouped.settings.theme;
			const posts: Post[] = screen.grouped.post;
			void posts;

			// The union narrows on the wrapper's type tag.
			const window = await activityFeed.query({ threadId: 't' }).list();
			for (const item of window.records) {
				switch (item.type) {
					case 'comment':
						void (item.record.body satisfies string);
						break;
					case 'reaction':
						void (item.record.emoji satisfies string);
						break;
					case 'edit':
						void (item.record.diff satisfies string);
						break;
				}
				// @ts-expect-error record is a union until type narrows it
				void item.record.body;
			}

			// Grouped and TaggedItem are exported and match the inferred shapes.
			const grouped: Grouped<typeof timelineMembers> = window.grouped;
			void grouped;
			const tagged: TaggedItem<typeof timelineMembers> | undefined =
				window.records[0];
			void tagged;
		};
		void _typeAssertions;
		expect(true).toBe(true);
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
