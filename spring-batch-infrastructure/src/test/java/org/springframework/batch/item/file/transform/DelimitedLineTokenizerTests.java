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

import static org.junit.Assert.*;

import org.springframework.batch.item.file.mapping.FieldSet;

public class DelimitedLineTokenizerTests {

	private static final String TOKEN_MATCHES = "token equals the expected string";

	private DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();

	@org.junit.Test
public void testTokenizeRegularUse() {
		FieldSet tokens = tokenizer.tokenize("sfd,\"Well,I have no idea what to do in the afternoon\",sFj, asdf,,as\n");
		assertEquals(6, tokens.getFieldCount());
		assertTrue(TOKEN_MATCHES, tokens.readString(0).equals("sfd"));
		assertTrue(TOKEN_MATCHES, tokens.readString(1).equals("Well,I have no idea what to do in the afternoon"));
		assertTrue(TOKEN_MATCHES, tokens.readString(2).equals("sFj"));
		assertTrue(TOKEN_MATCHES, tokens.readString(3).equals("asdf"));
		assertTrue(TOKEN_MATCHES, tokens.readString(4).equals(""));
		assertTrue(TOKEN_MATCHES, tokens.readString(5).equals("as"));

		tokens = tokenizer.tokenize("First string,");
		assertEquals(2, tokens.getFieldCount());
		assertTrue(TOKEN_MATCHES, tokens.readString(0).equals("First string"));
		assertTrue(TOKEN_MATCHES, tokens.readString(1).equals(""));
	}

	@org.junit.Test
public void testInvalidConstructorArgument() {
		try {
			new DelimitedLineTokenizer(DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER);
			fail("Quote character can't be used as delimiter for delimited line tokenizer!");
		}
		catch (Exception e) {
			assertTrue(true);
		}
	}

	@org.junit.Test
public void testDelimitedLineTokenizer() {
		FieldSet line = tokenizer.tokenize("a,b,c");
		assertEquals(3, line.getFieldCount());
	}

	@org.junit.Test
public void testNames() {
		tokenizer.setNames(new String[] {"A", "B", "C"});
		FieldSet line = tokenizer.tokenize("a,b,c");
		assertEquals(3, line.getFieldCount());
		assertEquals("a", line.readString("A"));
	}

	@org.junit.Test
public void testTooFewNames() {
		tokenizer.setNames(new String[] {"A", "B"});
		try {
			tokenizer.tokenize("a,b,c");
			fail("Expected IncorrectTokenCountException");
		}
		catch (IncorrectTokenCountException e) {
			assertEquals(2, e.getExpectedCount());
			assertEquals(3, e.getActualCount());
		}
	}
	
	@org.junit.Test
public void testTooManyNames() {
		tokenizer.setNames(new String[] {"A", "B", "C", "D"});
		try{
			tokenizer.tokenize("a,b,c");
		}
		catch(IncorrectTokenCountException e){
			assertEquals(4, e.getExpectedCount());
			assertEquals(3, e.getActualCount());
		}
		
	}

	@org.junit.Test
public void testDelimitedLineTokenizerChar() {
		AbstractLineTokenizer tokenizer = new DelimitedLineTokenizer(' ');
		FieldSet line = tokenizer.tokenize("a b c");
		assertEquals(3, line.getFieldCount());
	}

	@org.junit.Test
public void testTokenizeWithQuotes() {
		FieldSet line = tokenizer.tokenize("a,b,\"c\"");
		assertEquals(3, line.getFieldCount());
		assertEquals("c", line.readString(2));
	}

	@org.junit.Test
public void testTokenizeWithNotDefaultQuotes() {
		tokenizer.setQuoteCharacter('\'');
		FieldSet line = tokenizer.tokenize("a,b,'c'");
		assertEquals(3, line.getFieldCount());
		assertEquals("c", line.readString(2));
	}

	@org.junit.Test
public void testTokenizeWithEscapedQuotes() {
		FieldSet line = tokenizer.tokenize("a,\"\"b,\"\"\"c\"");
		assertEquals(3, line.getFieldCount());
		assertEquals("\"\"b", line.readString(1));
		assertEquals("\"c", line.readString(2));
	}

	@org.junit.Test
public void testTokenizeWithUnclosedQuotes() {
		tokenizer.setQuoteCharacter('\'');
		FieldSet line = tokenizer.tokenize("a,\"b,c");
		assertEquals(3, line.getFieldCount());
		assertEquals("\"b", line.readString(1));
		assertEquals("c", line.readString(2));
	}

	@org.junit.Test
public void testTokenizeWithSpaceAtEnd() {
		FieldSet line = tokenizer.tokenize("a,b,c ");
		assertEquals(3, line.getFieldCount());
		assertEquals("c", line.readString(2));
	}

	@org.junit.Test
public void testTokenizeWithQuoteAndSpaceAtEnd() {
		FieldSet line = tokenizer.tokenize("a,b,\"c\" ");
		assertEquals(3, line.getFieldCount());
		assertEquals("c", line.readString(2));
	}

	@org.junit.Test
public void testTokenizeWithQuoteAndSpaceBeforeDelimiter() {
		FieldSet line = tokenizer.tokenize("a,\"b\" ,c");
		assertEquals(3, line.getFieldCount());
		assertEquals("b", line.readString(1));
	}

	@org.junit.Test
public void testTokenizeWithDelimiterAtEnd() {
		FieldSet line = tokenizer.tokenize("a,b,c,");
		assertEquals(4, line.getFieldCount());
		assertEquals("c", line.readString(2));
		assertEquals("", line.readString(3));
	}

	@org.junit.Test
public void testEmptyLine() throws Exception {
		FieldSet line = tokenizer.tokenize("");
		assertEquals(0, line.getFieldCount());
	}
	
	@org.junit.Test
public void testEmptyLineWithNames(){
		
		tokenizer.setNames(new String[]{"A", "B"});
		try{
			tokenizer.tokenize("");
		}
		catch(IncorrectTokenCountException ex){
			assertEquals(2, ex.getExpectedCount());
			assertEquals(0, ex.getActualCount());
		}
	}

	@org.junit.Test
public void testWhitespaceLine() throws Exception {
		FieldSet line = tokenizer.tokenize("  ");
		// whitespace counts as text
		assertEquals(1, line.getFieldCount());
	}

	@org.junit.Test
public void testNullLine() throws Exception {
		FieldSet line = tokenizer.tokenize(null);
		// null doesn't...
		assertEquals(0, line.getFieldCount());
	}

	@org.junit.Test
public void testMultiLineField() throws Exception {
		FieldSet line = tokenizer.tokenize("a,b,c\nrap");
		assertEquals(3, line.getFieldCount());
		assertEquals("c\nrap", line.readString(2));

	}

	@org.junit.Test
public void testMultiLineFieldWithQuotes() throws Exception {
		FieldSet line = tokenizer.tokenize("a,b,\"c\nrap\"");
		assertEquals(3, line.getFieldCount());
		assertEquals("c\nrap", line.readString(2));

	}
	
	@org.junit.Test
public void testTokenizeWithQuotesEmptyValue() {
		FieldSet line = tokenizer.tokenize("\"a\",\"b\",\"\",\"d\"");
		assertEquals(4, line.getFieldCount());
		assertEquals("", line.readString(2));
	}
}
