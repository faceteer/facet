import { Facet, Index } from '@faceteer/facet';
import { dynamoDb, tableName } from './config.js';
import {
	ticketPickValidator,
	ticketValidator,
	watcherValidator,
	type Ticket,
	type Watcher,
} from './models.js';

const connection = { dynamoDb, tableName };

export const TicketFacet = new Facet({
	name: 'TICKET',
	validator: ticketValidator,
	pickValidator: ticketPickValidator,
	PK: {
		keys: ['orgId'],
		prefix: '#ORG',
	},
	SK: {
		keys: ['ticketId'],
		prefix: '#TICKET',
	},
	connection,
})
	.addIndex({
		index: Index.GSI1,
		alias: 'byStatusCreated',
		PK: {
			keys: ['orgId', 'status'],
			prefix: '#ORGSTATUS',
		},
		SK: {
			keys: ['createdAt'],
			prefix: '#CREATED',
		},
	})
	.addIndex({
		index: Index.GSI2,
		alias: 'byAssigneeDue',
		PK: {
			keys: ['orgId', 'assigneeId'],
			prefix: '#ORGASSIGNEE',
		},
		SK: {
			keys: ['dueAt'],
			prefix: '#DUE',
		},
	});

export const WatcherFacet = new Facet({
	name: 'WATCHER',
	validator: watcherValidator,
	PK: {
		keys: ['orgId'],
		prefix: '#ORG',
	},
	SK: {
		keys: ['watcherId'],
		prefix: '#WATCHER',
	},
	connection,
});

export type { Ticket, Watcher };
