import { DynamoDB } from '@aws-sdk/client-dynamodb';

export const tableName = process.env.TABLE_NAME ?? 'HELPDESK';

export const dynamoDb = new DynamoDB({
	region: 'local',
	endpoint: 'http://localhost:8000',
	credentials: {
		accessKeyId: 'test',
		secretAccessKey: 'test',
	},
});
