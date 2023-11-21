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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.UnsupportedCharsetException;

import static org.junit.Assert.*;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.mapping.DefaultFieldSet;
import org.springframework.batch.item.file.mapping.FieldSet;
import org.springframework.batch.item.file.mapping.FieldSetCreator;
import org.springframework.batch.item.file.mapping.PassThroughFieldSetMapper;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.ClassUtils;
import static org.junit.Assert.*;
/**
 * Tests of regular usage for {@link FlatFileItemWriter} Exception cases will be
 * in separate TestCase classes with different <code>setUp</code> and
 * <code>tearDown</code> methods
 * 
 * @author robert.kasanicky
 * @author Dave Syer
 * 
 */
public class FlatFileItemWriterTests {

	// object under test
	private FlatFileItemWriter writer = new FlatFileItemWriter();

	// String to be written into file by the FlatFileInputTemplate
	private static final String TEST_STRING = "FlatFileOutputTemplateTest-OutputData";

	// temporary output file
	private File outputFile;

	// reads the output file to check the result
	private BufferedReader reader;

	private ExecutionContext executionContext;

	/**
	 * Create temporary output file, define mock behaviour, set dependencies and
	 * initialize the object under test
	 */
	    @org.junit.Before
public void setUp() throws Exception {

		if (TransactionSynchronizationManager.isSynchronizationActive()) {
			TransactionSynchronizationManager.clearSynchronization();
		}
		TransactionSynchronizationManager.initSynchronization();

		outputFile = File.createTempFile("flatfile-output-", ".tmp");

		writer.setResource(new FileSystemResource(outputFile));
		writer.setFieldSetCreator(new PassThroughFieldSetMapper());
		writer.afterPropertiesSet();
		writer.setSaveState(true);
		executionContext = new ExecutionContext();
	}

	/**
	 * Release resources and delete the temporary output file
	 */
	@org.junit.After
    public void tearDown() throws Exception {
		if (reader != null) {
			reader.close();
		}
		writer.close(null);
		outputFile.delete();
	}

	/*
	 * Read a line from the output file, if the reader has not been created,
	 * recreate. This method is only necessary because running the tests in a
	 * UNIX environment locks the file if it's open for writing.
	 */
	private String readLine() throws IOException {

		if (reader == null) {
			reader = new BufferedReader(new FileReader(outputFile));
		}

		return reader.readLine();
	}
	
	@org.junit.Test
public void testWriteWithMultipleOpen() throws Exception{
		
		writer.open(executionContext);
		writer.write("test1");
		writer.flush();
		writer.open(executionContext);
		writer.write("test2");
		writer.flush();
		assertEquals("test1", readLine());
		assertEquals("test2", readLine());
	}
	
	@org.junit.Test
public void testOpenTwice(){
		//opening the writer twice should cause no issues
		writer.open(executionContext);
		writer.open(executionContext);
	}

	/**
	 * Regular usage of <code>write(String)</code> method
	 * 
	 * @throws Exception Exception
	 */
	@org.junit.Test
public void testWriteString() throws Exception {
		writer.open(executionContext);
		writer.write(TEST_STRING);
		writer.flush();
		writer.close(null);
		String lineFromFile = readLine();

		assertEquals(TEST_STRING, lineFromFile);
	}

	/**
	 * Regular usage of <code>write(String)</code> method
	 * 
	 * @throws Exception Exception
	 */
	@org.junit.Test
public void testWriteWithConverter() throws Exception {
		writer.setFieldSetCreator(new FieldSetCreator() {
			public FieldSet mapItem(Object data) {
				return new DefaultFieldSet(new String[] { "FOO:" + data });
			}
		});
		Object data = new Object();
		writer.open(executionContext);
		writer.write(data);
		writer.flush();
		String lineFromFile = readLine();
		// converter not used if input is String
		assertEquals("FOO:" + data.toString(), lineFromFile);
	}

	/**
	 * Regular usage of <code>write(String)</code> method
	 * 
	 * @throws Exception Exception
	 */
	@org.junit.Test
public void testWriteWithConverterAndInfiniteLoop() throws Exception {
		writer.setFieldSetCreator(new FieldSetCreator() {
			public FieldSet mapItem(Object data) {
				return new DefaultFieldSet(new String[] { "FOO:" + data });
			}
		});
		Object data = new Object();
		writer.open(executionContext);
		writer.write(data);
		writer.flush();
		String lineFromFile = readLine();
		// converter not used if input is String
		assertEquals("FOO:" + data.toString(), lineFromFile);
	}

	/**
	 * Regular usage of <code>write(String)</code> method
	 * 
	 * @throws Exception Exception
	 */
	@org.junit.Test
public void testWriteWithConverterAndString() throws Exception {
		writer.setFieldSetCreator(new FieldSetCreator() {
			public FieldSet mapItem(Object data) {
				return new DefaultFieldSet(new String[] { "FOO:" + data });
			}
		});
		writer.open(executionContext);
		writer.write(TEST_STRING);
		writer.flush();
		String lineFromFile = readLine();
		assertEquals("FOO:" + TEST_STRING, lineFromFile);
	}

	/**
	 * Regular usage of <code>write(String[], LineDescriptor)</code> method
	 * 
	 * @throws Exception Exception
	 */
	@org.junit.Test
public void testWriteRecord() throws Exception {
		String args = "1";
		writer.open(executionContext);
		writer.write(args);
		writer.flush();
		String lineFromFile = readLine();
		assertEquals(args, lineFromFile);
	}

	@org.junit.Test
public void testWriteRecordWithrecordSeparator() throws Exception {
		writer.setLineSeparator("|");
		writer.open(executionContext);
		writer.write("1");
		writer.write("2");
		writer.flush();
		String lineFromFile = readLine();
		assertEquals("1|2|", lineFromFile);
	}

	@org.junit.Test
public void testRollback() throws Exception {
		writer.open(executionContext);
		writer.write("testLine1");
		// rollback
		rollback();
		writer.flush();
		writer.close(null);
		String lineFromFile = readLine();
		assertEquals(null, lineFromFile);
	}

	@org.junit.Test
public void testCommit() throws Exception {
		writer.open(executionContext);
		writer.write("testLine1");
		// rollback
		commit();
		writer.close(null);
		String lineFromFile = readLine();
		assertEquals("testLine1", lineFromFile);
	}

	@org.junit.Test
public void testRestart() throws Exception {
		
		writer.open(executionContext);
		// write some lines
		writer.write("testLine1");
		writer.write("testLine2");
		writer.write("testLine3");

		// commit
		commit();

		// this will be rolled back...
		writer.write("this will be rolled back");

		// rollback
		rollback();

		// write more lines
		writer.write("testLine4");
		writer.write("testLine5");

		// commit
		commit();

		// get restart data
		writer.update(executionContext);
		// close template
		writer.close(executionContext);

		// init with correct data
		writer.open(executionContext);

		// write more lines
		writer.write("testLine6");
		writer.write("testLine7");
		writer.write("testLine8");

		commit();

		// get statistics
		writer.update(executionContext);
		// close template
		writer.close(executionContext);

		// verify what was written to the file
		for (int i = 1; i < 9; i++) {
			assertEquals("testLine" + i, readLine());
		}

		// 3 lines were written to the file after restart
		assertEquals(3, executionContext.getLong(ClassUtils.getShortName(FlatFileItemWriter.class) + ".written"));

	}
	
	@org.junit.Test
public void testOpenWithNonWritableFile() throws Exception {
		writer = new FlatFileItemWriter();
		writer.setFieldSetCreator(new PassThroughFieldSetMapper());
		FileSystemResource file = new FileSystemResource("target/no-such-file.foo");
		writer.setResource(file);
		file.getFile().createNewFile();
		file.getFile().setReadOnly();
		writer.afterPropertiesSet();
		try {
			writer.open(executionContext);
			fail("Expected IllegalStateException");
		} catch (IllegalStateException e) {
			String message = e.getMessage();
			assertTrue("Message does not contain 'writable': "+message, message.indexOf("writable")>=0);
		}
	}

	@org.junit.Test
public void testAfterPropertiesSetChecksMandatory() throws Exception {
		writer = new FlatFileItemWriter();
		try {
			writer.afterPropertiesSet();
			fail("Expected IllegalArgumentException");
		}
		catch (IllegalArgumentException e) {
			// expected
		}
	}

	@org.junit.Test
public void testDefaultStreamContext() throws Exception {
		writer = new FlatFileItemWriter();
		writer.setResource(new FileSystemResource(outputFile));
		writer.setFieldSetCreator(new PassThroughFieldSetMapper());
		writer.afterPropertiesSet();
		writer.setSaveState(true);
		writer.open(executionContext);
		writer.update(executionContext);
		assertNotNull(executionContext);
		assertEquals(3, executionContext.entrySet().size());
		assertEquals(0, executionContext.getLong(ClassUtils.getShortName(FlatFileItemWriter.class) + ".current.count"));
	}

	/**
	 * Regular usage of <code>write(String)</code> method
	 * 
	 * @throws Exception Exception
	 */
	@org.junit.Test
public void testWriteStringWithBogusEncoding() throws Exception {
		writer.setEncoding("BOGUS");
		try {
			writer.open(executionContext);
			fail("Expecyted ItemStreamException");
		}
		catch (ItemStreamException e) {
			assertTrue(e.getCause() instanceof UnsupportedCharsetException);
		}
		writer.close(null);
	}

	/**
	 * Regular usage of <code>write(String)</code> method
	 * 
	 * @throws Exception Exception
	 */
	@org.junit.Test
public void testWriteStringWithEncodingAfterClose() throws Exception {
		testWriteStringWithBogusEncoding();
		writer.setEncoding("UTF-8");
		writer.open(executionContext);
		writer.write(TEST_STRING);
		writer.flush();
		String lineFromFile = readLine();

		assertEquals(TEST_STRING, lineFromFile);
	}

	@org.junit.Test
public void testWriteHeader() throws Exception {
		writer.setHeaderLines(new String[] {"a", "b"});
		writer.open(executionContext);
		writer.write(TEST_STRING);
		writer.flush();
		writer.close(null);
		String lineFromFile = readLine();
		assertEquals("a", lineFromFile);
		lineFromFile = readLine();
		assertEquals("b", lineFromFile);
		lineFromFile = readLine();
		assertEquals(TEST_STRING, lineFromFile);
	}

	@org.junit.Test
public void testWriteHeaderAfterRestartOnFirstChunk() throws Exception {
		writer.setHeaderLines(new String[] {"a", "b"});
		writer.open(executionContext);
		writer.write(TEST_STRING);
		writer.clear();
		writer.close(executionContext);
		writer.open(executionContext);
		writer.write(TEST_STRING);
		writer.flush();
		writer.close(executionContext);
		String lineFromFile = readLine();
		assertEquals("a", lineFromFile);
		lineFromFile = readLine();
		assertEquals("b", lineFromFile);
		lineFromFile = readLine();
		assertEquals(TEST_STRING, lineFromFile);
		lineFromFile = readLine();
		assertEquals(null, lineFromFile);
	}

	@org.junit.Test
public void testWriteHeaderAfterRestartOnSecondChunk() throws Exception {
		writer.setHeaderLines(new String[] {"a", "b"});
		writer.open(executionContext);
		writer.write(TEST_STRING);
		writer.flush();
		writer.update(executionContext);
		writer.write(TEST_STRING);
		writer.clear();
		writer.close(executionContext);
		String lineFromFile = readLine();
		assertEquals("a", lineFromFile);
		lineFromFile = readLine();
		assertEquals("b", lineFromFile);
		lineFromFile = readLine();
		assertEquals(TEST_STRING, lineFromFile);
		writer.open(executionContext);
		writer.write(TEST_STRING);
		writer.flush();
		writer.close(executionContext);
		reader = null;
		lineFromFile = readLine();
		assertEquals("a", lineFromFile);
		lineFromFile = readLine();
		assertEquals("b", lineFromFile);
		lineFromFile = readLine();
		assertEquals(TEST_STRING, lineFromFile);
		lineFromFile = readLine();
		assertEquals(TEST_STRING, lineFromFile);
	}

	private void commit() throws Exception {
		writer.flush();
	}

	private void rollback() throws Exception {
		writer.clear();
	}

}
