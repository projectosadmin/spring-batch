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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.*;

import org.springframework.batch.item.file.mapping.DefaultFieldSet;
import org.springframework.batch.item.file.mapping.FieldSet;

public class PrefixMatchingCompositeLineTokenizerTests {

	PrefixMatchingCompositeLineTokenizer tokenizer = new PrefixMatchingCompositeLineTokenizer();
	
	@org.junit.Test
public void testNoTokenizers() throws Exception {
		try {
			tokenizer.tokenize("a line");
			fail("Expected IllegalStateException");
		} catch (IllegalStateException e) {
			// expected
		}
	}
	
	@org.junit.Test
public void testNullLine() throws Exception {
		tokenizer.setTokenizers(Collections.singletonMap("foo", new DelimitedLineTokenizer())); 
		FieldSet fields = tokenizer.tokenize(null);
		assertEquals(0, fields.getFieldCount());
	}
	
	@org.junit.Test
public void testEmptyKeyMatchesAnyLine() throws Exception {
		Map map = new HashMap();
		map.put("", new DelimitedLineTokenizer());
		map.put("foo", new LineTokenizer() {
			public FieldSet tokenize(String line) {
				return null;
			}
		});
		tokenizer.setTokenizers(map); 
		FieldSet fields = tokenizer.tokenize("abc");
		assertEquals(1, fields.getFieldCount());
	}

	@org.junit.Test
public void testEmptyKeyDoesNotMatchWhenAlternativeAvailable() throws Exception {
		
		Map map = new LinkedHashMap();
		map.put("", new LineTokenizer() {
			public FieldSet tokenize(String line) {
				return null;
			}
		});
		map.put("foo", new DelimitedLineTokenizer());
		tokenizer.setTokenizers(map); 
		FieldSet fields = tokenizer.tokenize("foo,bar");
		assertEquals("bar", fields.readString(1));
	}

	@org.junit.Test
public void testNoMatch() throws Exception {
		tokenizer.setTokenizers(Collections.singletonMap("foo", new DelimitedLineTokenizer())); 
		try {
			tokenizer.tokenize("nomatch");
			fail("Expected IllegalStateException");
		} catch (IllegalStateException e) {
			// expected
		}
	}
	
	@org.junit.Test
public void testMatchWithPrefix() throws Exception {
		tokenizer.setTokenizers(Collections.singletonMap("foo", new LineTokenizer() {
			public FieldSet tokenize(String line) {
				return new DefaultFieldSet(new String[] {line});
			}
		}));
		FieldSet fields = tokenizer.tokenize("foo bar");
		assertEquals(1, fields.getFieldCount());
		assertEquals("foo bar", fields.readString(0));
	}
}
