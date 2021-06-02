module.exports = {
	parser: '@typescript-eslint/parser',
	extends: ['google', 'eslint:recommended', 'prettier'],
	plugins: ['@typescript-eslint', 'prettier'],
	env: {
		es6: true,
		browser: false,
		node: true,
	},
	rules: {
		'require-jsdoc': 'off',
		'valid-jsdoc': 'off',
		semi: 'off',
		'no-unused-vars': 'off',
		'@typescript-eslint/no-unused-vars': 'error',
		'prettier/prettier': [
			'error',
			{
				semi: true,
				singleQuote: true,
				useTabs: true,
				trailingComma: 'all',
				bracketSpacing: true,
			},
		],
	},
};
