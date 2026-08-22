export interface ConverterOptions {
	/**
	 * An optional flag indicating that the document client should cast
	 * empty strings, buffers, and sets to NULL shapes
	 */
	convertEmptyValues?: boolean;

	/**
	 * Whether to return numbers as a NumberValue object instead of
	 * converting them to native JavaScript numbers. This allows for the
	 * safe round-trip transport of numbers of arbitrary size.
	 */
	wrapNumbers?: boolean;

	/**
	 * Whether to convert Date objects to an ISO string or
	 * a unix timestamp.
	 *
	 * The default is `iso`
	 */
	dateFormat?: 'iso' | 'unix';
}
