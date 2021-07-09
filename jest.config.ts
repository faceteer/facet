export default {
	preset: 'ts-jest',
	testEnvironment: 'node',
	reporters: ['default', 'jest-junit'],
	testPathIgnorePatterns: ['^.+\\.js$'],
	globals: {
		'ts-jest': {
			isolatedModules: true,
		},
	},
};
