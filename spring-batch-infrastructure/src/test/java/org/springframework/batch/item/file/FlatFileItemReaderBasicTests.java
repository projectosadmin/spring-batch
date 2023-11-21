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

package org.springframework.batch.item.file;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.*;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ReaderNotOpenException;
import org.springframework.batch.item.file.mapping.DefaultFieldSet;
import org.springframework.batch.item.file.mapping.FieldSet;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import static org.junit.Assert.*;
/**
 * Tests for {@link FlatFileItemReader} - the fundamental item reading functionality.
 * 
 * @see FlatFileItemReaderAdvancedTests
 * @author Dave Syer
 */
public class FlatFileItemReaderBasicTests {

	// object under test
	private FlatFileItemReader itemReader = new FlatFileItemReader();

	// common value used for writing to a file
	private String TEST_STRING = "FlatFileInputTemplate-TestData";
	private String TEST_OUTPUT = "[FlatFileInputTemplate-TestData]";

	private ExecutionContext executionContext;

	// simple stub instead of a realistic tokenizer
	private LineTokenizer tokenizer = new LineTokenizer() {
		public FieldSet tokenize(String line) {
			return new DefaultFieldSet(new String[] { line });
		}
	};

	private FieldSetMapper fieldSetMapper = new FieldSetMapper() {
		public Object mapLine(FieldSet fs) {
			return fs;
		}
	};

	/**
	 * Create inputFile, inject mock/stub dependencies for tested object, initialize the tested object
	 */
	    @org.junit.Before
public void setUp() throws Exception {

		itemReader.setResource(getInputResource(TEST_STRING));
		itemReader.setLineTokenizer(tokenizer);
		itemReader.setFieldSetMapper(fieldSetMapper);
		itemReader.afterPropertiesSet();

		executionContext = new ExecutionContext();
	}

	/**
	 * Release resources.
	 */
	@org.junit.After
    public void tearDown() throws Exception {
		itemReader.close(null);
	}

	private Resource getInputResource(String input) {
		return new ByteArrayResource(input.getBytes());
	}

	/**
	 * Regular usage of <code>read</code> method
	 */
	@org.junit.Test
public void testRead() throws Exception {
		itemReader.open(executionContext);
		assertEquals("[FlatFileInputTemplate-TestData]", itemReader.read().toString());
	}

	/**
	 * Regular usage of <code>read</code> method
	 */
	@org.junit.Test
public void testReadExhausted() throws Exception {
		itemReader.open(executionContext);
		assertEquals("[FlatFileInputTemplate-TestData]", itemReader.read().toString());
		assertEquals(null, itemReader.read());
	}

	/**
	 * Regular usage of <code>read</code> method
	 */
	@org.junit.Test
public void testReadWithTokenizerError() throws Exception {
		itemReader.setLineTokenizer(new LineTokenizer() {
			public FieldSet tokenize(String line) {
				throw new RuntimeException("foo");
			}
		});
		try {
			itemReader.open(executionContext);
			itemReader.read();
			fail("Expected ParsingException");
		} catch (FlatFileParseException e) {
			assertEquals(e.getInput(), TEST_STRING);
			assertEquals(e.getLineNumber(), 1);
		}
	}

	@org.junit.Test
public void testReadWithMapperError() throws Exception {
		itemReader.setFieldSetMapper(new FieldSetMapper() {
			public Object mapLine(FieldSet fs) {
				throw new RuntimeException("foo");
			}
		});

		try {
			itemReader.open(executionContext);
			itemReader.read();
			fail("Expected ParsingException");
		} catch (FlatFileParseException e) {
			assertEquals(e.getInput(), TEST_STRING);
			assertEquals(e.getLineNumber(), 1);
		}
	}

	@org.junit.Test
public void testReadBeforeOpen() throws Exception {
		itemReader = new FlatFileItemReader();
		itemReader.setResource(getInputResource(TEST_STRING));
		itemReader.setFieldSetMapper(fieldSetMapper);
		try {
			itemReader.read();
			fail("Expected ReaderNotOpenException");
		} catch (ReaderNotOpenException e) {
			assertTrue(contains(e.getMessage(), "open"));
		}
	}
	
	@org.junit.Test
public void testResourceUnavailableAfterPropertiesSet() throws Exception{
		
		itemReader.setResource(null);
		itemReader.afterPropertiesSet();
		//no exception should be thrown
	}
	
	@org.junit.Test
public void testNullResourceInOpen() throws Exception{
		
		itemReader.setResource(null);
		try{
			itemReader.open(new ExecutionContext());
			fail();
		}
		catch(Exception ex){
			//expected
		}
	}
	

	@org.junit.Test
public void testCloseBeforeOpen() throws Exception {
		itemReader = new FlatFileItemReader();
		itemReader.setResource(getInputResource(TEST_STRING));
		itemReader.setFieldSetMapper(fieldSetMapper);
		itemReader.close(null);
		// The open does not happen automatically on a read...
		itemReader.open(executionContext);
		assertEquals("[FlatFileInputTemplate-TestData]", itemReader.read().toString());
	}

	@org.junit.Test
public void testInitializationWithNullResource() throws Exception {
		itemReader = new FlatFileItemReader();
		try {
			itemReader.afterPropertiesSet();
			fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			// expected
		}
	}

	@org.junit.Test
public void testOpenTwiceHasNoEffect() throws Exception {
		itemReader.open(executionContext);
		testRead();
	}

	@org.junit.Test
public void testSetValidEncoding() throws Exception {
		itemReader = new FlatFileItemReader();
		itemReader.setEncoding("UTF-8");
		itemReader.setResource(getInputResource(TEST_STRING));
		itemReader.setFieldSetMapper(fieldSetMapper);
		itemReader.open(executionContext);
		testRead();
	}

	@org.junit.Test
public void testSetNullEncoding() throws Exception {
		itemReader = new FlatFileItemReader();
		itemReader.setEncoding(null);
		itemReader.setResource(getInputResource(TEST_STRING));
		try {
			itemReader.open(executionContext);
			fail("Expected IllegalArgumentException");
		} catch (ItemStreamException e) {
			// expected
		}
	}

	@org.junit.Test
public void testSetInvalidEncoding() throws Exception {
		itemReader = new FlatFileItemReader();
		itemReader.setEncoding("foo");
		itemReader.setResource(getInputResource(TEST_STRING));
		try {
			itemReader.open(executionContext);
			fail("Expected BatchEnvironmentException");
		} catch (ItemStreamException e) {
			// expected
			assertEquals("Failed to initialize the reader", e.getMessage());
			assertEquals("foo", e.getCause().getCause().getMessage());
		}
	}

	@org.junit.Test
public void testEncoding() throws Exception {
		itemReader.setEncoding("UTF-8");
		testRead();
	}

	@org.junit.Test
public void testRecordSeparator() throws Exception {
		itemReader.setRecordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
		testRead();
	}

	@org.junit.Test
public void testComments() throws Exception {
		itemReader.setResource(getInputResource("% Comment\n" + TEST_STRING));
		itemReader.setComments(new String[] { "%" });
		testRead();
	}

	/**
	 * Header line is skipped and used to setup fieldSet column names.
	 */
	@org.junit.Test
public void testColumnNamesInHeader() throws Exception {
		final String INPUT = "name1|name2\nvalue1|value2\nvalue3|value4";

		itemReader = new FlatFileItemReader();
		itemReader.setResource(getInputResource(INPUT));
		itemReader.setLineTokenizer(new DelimitedLineTokenizer('|'));
		itemReader.setFieldSetMapper(fieldSetMapper);
		itemReader.setFirstLineIsHeader(true);
		itemReader.afterPropertiesSet();
		itemReader.open(executionContext);

		FieldSet fs = (FieldSet) itemReader.read();
		assertEquals("value1", fs.readString("name1"));
		assertEquals("value2", fs.readString("name2"));

		fs = (FieldSet) itemReader.read();
		assertEquals("value3", fs.readString("name1"));
		assertEquals("value4", fs.readString("name2"));
	}

	/**
	 * Header line is skipped and used to setup fieldSet column names.
	 */
	@org.junit.Test
public void testLinesToSkip() throws Exception {
		final String INPUT = "foo bar spam\none two\nthree four";

		itemReader = new FlatFileItemReader();
		itemReader.setResource(getInputResource(INPUT));
		itemReader.setLineTokenizer(new DelimitedLineTokenizer(' '));
		itemReader.setFieldSetMapper(fieldSetMapper);
		itemReader.setLinesToSkip(1);
		itemReader.afterPropertiesSet();
		itemReader.open(executionContext);

		FieldSet fs = (FieldSet) itemReader.read();
		assertEquals("one", fs.readString(0));
		assertEquals("two", fs.readString(1));

		fs = (FieldSet) itemReader.read();
		assertEquals("three", fs.readString(0));
		assertEquals("four", fs.readString(1));
	}

	@org.junit.Test
public void testNonExistantResource() throws Exception {

		Resource resource = new NonExistentResource();

		FlatFileItemReader testReader = new FlatFileItemReader();
		testReader.setResource(resource);
		testReader.setLineTokenizer(tokenizer);
		testReader.setFieldSetMapper(fieldSetMapper);
		testReader.setResource(resource);

		// afterPropertiesSet should only throw an exception if the Resource is null
		testReader.afterPropertiesSet();

		
		testReader.open(executionContext);
		assertNull(testReader.read());
		

	}

	@org.junit.Test
public void testRuntimeFileCreation() throws Exception {

		Resource resource = new NonExistentResource();

		FlatFileItemReader testReader = new FlatFileItemReader();
		testReader.setResource(resource);
		testReader.setLineTokenizer(tokenizer);
		testReader.setFieldSetMapper(fieldSetMapper);
		testReader.setResource(resource);

		// afterPropertiesSet should only throw an exception if the Resource is null
		testReader.afterPropertiesSet();

		// replace the resource to simulate runtime resource creation
		testReader.setResource(getInputResource(TEST_STRING));
		testReader.open(executionContext);
		assertEquals(TEST_OUTPUT, testReader.read().toString());
	}

	private boolean contains(String str, String searchStr) {
		return str.indexOf(searchStr) != -1;
	}

	private static class NonExistentResource extends AbstractResource {

		public NonExistentResource() {
		}

		public boolean exists() {
			return false;
		}

		public String getDescription() {
			return "NonExistantResource";
		}

		public InputStream getInputStream() throws IOException {
			return null;
		}
	}
}
