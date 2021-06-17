import { DynamoDB } from 'aws-sdk';
import { Facet } from './facet';

export enum TokenStatus {
	Active = 'active',
	Failed = 'failed',
}

export interface Page {
	orgId: string;
	pageId: string;
	pageName: string;
	accessToken: string;
	tokenStatus: TokenStatus;
	publishingDisabled?: boolean;
}

export const PageFacet = new Facet({
	PK: {
		keys: ['orgId'],
		prefix: '#ORG',
	},
	SK: {
		keys: ['pageId'],
		prefix: '#FBPG',
	},
	validator: (input: unknown): Page => {
		return input as Page;
	},
	connection: {
		dynamoDb: new DynamoDB(),
		tableName: 'something',
	},
	indexes: {
		GSI1: {
			PK: {
				keys: ['pageId'],
				prefix: '#FBPG',
			},
			SK: {
				keys: ['pageId'],
				prefix: '#FBPG',
			},
		},
	},
});
