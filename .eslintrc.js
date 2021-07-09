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
		'no-undef': 'off',
		'require-jsdoc': 'off',
		'valid-jsdoc': 'off',
		semi: 'off',
		'no-unused-vars': 'off',
		'@typescript-eslint/no-unused-vars': 'error',
		'no-dupe-class-members': 'off',
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
