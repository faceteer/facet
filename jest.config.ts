export default {
	preset: 'ts-jest',
	testEnvironment: 'node',
	reporters: ['default', 'jest-junit'],
	testPathIgnorePatterns: ['^.+\\.js$'],
	transform: {
		'^.+\\.ts$': ['ts-jest'],
	},
};
