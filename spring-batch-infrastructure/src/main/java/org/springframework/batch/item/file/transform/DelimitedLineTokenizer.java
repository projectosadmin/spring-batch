/*
 * Copyright 2006-2007 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.item.file.transform;

import java.util.ArrayList;
import java.util.List;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * 
 * @author Rob Harrop
 * @author Dave Syer
 * 
 */
public class DelimitedLineTokenizer extends AbstractLineTokenizer {
	/**
	 * Convenient constant for the common case of a tab delimiter.
	 */
	public static final char DELIMITER_TAB = '\t';

	/**
	 * Convenient constant for the common case of a comma delimiter.
	 */
	public static final char DELIMITER_COMMA = ',';

	/**
	 * Convenient constant for the common case of a " character used to escape delimiters or line endings.
	 */
	public static final char DEFAULT_QUOTE_CHARACTER = '"';

	// the delimiter character used when reading input.
	private char delimiter;

	private char quoteCharacter = DEFAULT_QUOTE_CHARACTER;

	private String quoteString;

	/**
	 * Create a new instance of the {@link DelimitedLineTokenizer} class for the common case where the delimiter is a
	 * {@link #DELIMITER_COMMA comma}.
	 * 
	 * @see #DelimitedLineTokenizer(char)
	 * @see #DELIMITER_COMMA
	 */
	public DelimitedLineTokenizer() {
		this(DELIMITER_COMMA);
	}

	/**
	 * Create a new instance of the {@link DelimitedLineTokenizer} class.
	 * 
	 * @param delimiter the desired delimiter
	 */
	public DelimitedLineTokenizer(char delimiter) {
		Assert.state(delimiter != DEFAULT_QUOTE_CHARACTER, "[" + DEFAULT_QUOTE_CHARACTER
		        + "] is not allowed as delimiter for tokenizers.");

		this.delimiter = delimiter;
		setQuoteCharacter(DEFAULT_QUOTE_CHARACTER);
	}

	/**
	 * Setter for the delimiter character.
	 * 
	 * @param delimiter delimiter
	 */
	public void setDelimiter(char delimiter) {
		this.delimiter = delimiter;
	}

	/**
	 * Public setter for the quoteCharacter. The quote character can be used to extend a field across line endings or to
	 * enclose a String which contains the delimiter. Inside a quoted token the quote character can be used to escape
	 * itself, thus "a""b""c" is tokenized to a"b"c.
	 * 
	 * @param quoteCharacter the quoteCharacter to set
	 * 
	 * @see #DEFAULT_QUOTE_CHARACTER
	 */
	public void setQuoteCharacter(char quoteCharacter) {
		this.quoteCharacter = quoteCharacter;
		this.quoteString = "" + quoteCharacter;
	}

	/**
	 * Yields the tokens resulting from the splitting of the supplied <code>line</code>.
	 * 
	 * @param line the line to be tokenized
	 * 
	 * @return the resulting tokens
	 */
	protected List doTokenize(String line) {

		List tokens = new ArrayList();

		// line is never null in current implementation
		// line is checked in parent: AbstractLineTokenizer.tokenize()
		char[] chars = line.toCharArray();
		boolean inQuoted = false;
		int lastCut = 0;
		int length = chars.length;

		for (int i = 0; i < length; i++) {

			char currentChar = chars[i];
			boolean isEnd = (i == (length - 1));

			if ((isDelimiterCharacter(currentChar) && !inQuoted) || isEnd) {
				int endPosition = (isEnd ? (length - lastCut) : (i - lastCut));

				if (isEnd && isDelimiterCharacter(currentChar)) {
					endPosition--;
				}

				String value = null;

				value = maybeStripQuotes(new String(chars, lastCut, endPosition));

				tokens.add(value);

				if (isEnd && (isDelimiterCharacter(currentChar))) {
					tokens.add("");
				}

				lastCut = i + 1;
			} else if (isQuoteCharacter(currentChar)) {
				inQuoted = !inQuoted;
			}

		}

		return tokens;
	}

	/**
	 * If the string is quoted strip (possibly with whitespace outside the quotes (which will be stripped), replace
	 * escaped quotes inside the string. Quotes are escaped with double instances of the quote character.
	 * 
	 * @param string string
	 * @return the same string but stripped and unescaped if necessary
	 */
	private String maybeStripQuotes(String string) {
		String value = string.trim();
		if (isQuoted(value)) {
			value = StringUtils.replace(value, "" + quoteCharacter + quoteCharacter, "" + quoteCharacter);
			int endLength = value.length() - 1;
			// used to deal with empty quoted values
			if (endLength == 0) {
				endLength = 1;
			}
			string = value.substring(1, endLength);
		}
		return string;
	}

	/**
	 * Is this string surrounded by quite characters?
	 * 
	 * @param value value
	 * @return true if the value starts and ends with the {@link #quoteCharacter}
	 */
	private boolean isQuoted(String value) {
		if (value.startsWith(quoteString) && value.endsWith(quoteString)) {
			return true;
		}
		return false;
	}

	/**
	 * Is the supplied character the delimiter character?
	 * 
	 * @param c the character to be checked
	 * @return <code>true</code> if the supplied character is the delimiter character
	 * @see DelimitedLineTokenizer#DelimitedLineTokenizer(char)
	 */
	private boolean isDelimiterCharacter(char c) {
		return c == this.delimiter;
	}

	/**
	 * Is the supplied character a quote character?
	 * 
	 * @param c the character to be checked
	 * @return <code>true</code> if the supplied character is an quote character
	 * @see #setQuoteCharacter(char)
	 */
	protected boolean isQuoteCharacter(char c) {
		return c == quoteCharacter;
	}
}
