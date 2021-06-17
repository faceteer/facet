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
				keys: ['orgId'],
				prefix: '#ORG',
			},
			SK: {
				keys: ['tokenStatus', 'pageId'],
				prefix: '#FBPG',
			},
		},
	},
});

PageFacet.GSI1.query({ orgId: '47527' })
	.greaterThan(
		{
			tokenStatus: TokenStatus.Active,
			pageId: '24274',
		},
		{ filter: ['accessToken', 'between', '2424', '7727'] },
	)
	.then((result) => {
		console.log(result.records);
	});
