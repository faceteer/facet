/**
 * Seeds the helpdesk table with deterministic sample data.
 *
 * Run with: npx tsx seed.ts
 */
import { TicketFacet, WatcherFacet } from './src/facets.js';
import type { Ticket, Watcher } from './src/models.js';
import { ticketStatuses } from './src/models.js';

const HOUR = 60 * 60 * 1000;
const base = new Date('2026-06-01T00:00:00.000Z').getTime();

const tickets: Ticket[] = [];
for (let i = 0; i < 300; i++) {
	const ticket: Ticket = {
		orgId: 'acme',
		ticketId: `t${String(i).padStart(3, '0')}`,
		subject: `Ticket number ${i}`,
		status: ticketStatuses[i % 3],
		priority: i % 25 === 24 ? 5 : (i % 4) + 1,
		createdAt: new Date(base + ((i * 37) % 300) * HOUR),
		dueAt: new Date(base + (i + 100) * HOUR),
	};
	if (i % 2 === 0) {
		ticket.assigneeId = `agent-${(i / 2) % 4}`;
	}
	tickets.push(ticket);
}
for (let i = 0; i < 5; i++) {
	tickets.push({
		orgId: 'globex',
		ticketId: `g${i}`,
		subject: `Globex ticket ${i}`,
		status: 'open',
		priority: 3,
		createdAt: new Date(base + i * HOUR),
		dueAt: new Date(base + (i + 200) * HOUR),
	});
}

const watchers: Watcher[] = [];
for (let i = 0; i < 40; i++) {
	watchers.push({
		orgId: 'acme',
		watcherId: `w${String(i).padStart(2, '0')}`,
		email: `watcher${i}@example.com`,
		createdAt: new Date(base + i * HOUR),
	});
}
for (let i = 0; i < 5; i++) {
	watchers.push({
		orgId: 'globex',
		watcherId: `gw${i}`,
		email: `globex-watcher${i}@example.com`,
		createdAt: new Date(base + i * HOUR),
	});
}

const ticketResult = await TicketFacet.put(tickets);
const watcherResult = await WatcherFacet.put(watchers);

if (ticketResult.hasFailures || watcherResult.hasFailures) {
	console.error('Seed had failures', {
		tickets: ticketResult.failed,
		watchers: watcherResult.failed,
	});
	process.exit(1);
}

console.log(
	`Seeded ${ticketResult.put.length} tickets and ${watcherResult.put.length} watchers.`,
);
