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

import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.separator.ResourceLineReader;
import org.springframework.batch.item.file.separator.SuffixRecordSeparatorPolicy;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import static org.junit.Assert.*;
/**
 * @author Rob Harrop
 */
public class ResourceLineReaderTests {

	@org.junit.Test
public void testBadResource() throws Exception {
		ResourceLineReader reader = new ResourceLineReader(new InputStreamResource(new InputStream() {
			public int read() throws IOException {
				throw new IOException("Foo");
			}
		}));
		try {
			reader.read();
			fail("Expected UnexpectedInputException");
		}
		catch (UnexpectedInputException e) {
			// expected
			assertTrue(e.getMessage().startsWith("Unable to read"));
		}
	}

	@org.junit.Test
public void testRead() throws Exception {
		Resource resource = new ByteArrayResource("a,b,c\n1,2,3".getBytes());
		ResourceLineReader reader = new ResourceLineReader(resource);
		int count = 0;
		String line;
		while ((line = (String) reader.read()) != null) {
			count++;
			assertNotNull(line);
		}

		assertEquals(2, count);
	}
	
	@org.junit.Test
public void testCloseTwice() throws Exception {
		Resource resource = new ByteArrayResource("a,b,c\n1,2,3".getBytes());
		ResourceLineReader reader = new ResourceLineReader(resource);
		reader.open();
		reader.close();
		try {
			reader.close(); // just closing a BufferedReader twice should be fine
		} catch (Exception e) {
			fail("Unexpected Exception "+e);
		}
		assertEquals("a,b,c", reader.read());
	}
	
	@org.junit.Test
public void testEncoding() throws Exception {
		Resource resource = new ByteArrayResource("a,b,c\n1,2,3".getBytes());
		ResourceLineReader reader = new ResourceLineReader(resource, "UTF-8");
		int count = 0;
		String line;
		while ((line = (String) reader.read()) != null) {
			count++;
			assertNotNull(line);
		}

		assertEquals(2, count);		
	}
	
	@org.junit.Test
public void testLineCount() throws Exception {
		Resource resource = new ByteArrayResource("1,2,\"3\n4\"\n5,6,7".getBytes());
		ResourceLineReader reader = new ResourceLineReader(resource);
		reader.read();	
		assertEquals(2, reader.getPosition());
	}

  @org.junit.Test
public void testLineContent() throws Exception {
    Resource resource = new ByteArrayResource("1,2,3\n4\n5,6,7".getBytes());
    ResourceLineReader reader = new ResourceLineReader(resource);
    assertEquals("1,2,3", reader.read());
    assertEquals("4", reader.read());
    assertEquals("5,6,7", reader.read());
  }

  @org.junit.Test
public void testLineContentWhenLineContainsQuotedNewline() throws Exception {
	    Resource resource = new ByteArrayResource("1,2,\"3\n4\"\n5,6,7".getBytes());
	    ResourceLineReader reader = new ResourceLineReader(resource);
	    assertEquals("1,2,\"3\n4\"", reader.read());
	    assertEquals("5,6,7", reader.read());
	  }

  @org.junit.Test
public void testLineEndings() throws Exception {
		Resource resource = new ByteArrayResource("1\n2\r\n3".getBytes());
		ResourceLineReader reader = new ResourceLineReader(resource);
		reader.read();
		String line = (String) reader.read();
		assertEquals("2", line);
		assertEquals(2, reader.getPosition());
		line = (String) reader.read();
		assertEquals("3", line);
		assertEquals(3, reader.getPosition());
	}

	@org.junit.Test
public void testDefaultComments() throws Exception {
		Resource resource = new ByteArrayResource("1\n# 2\n3".getBytes());
		ResourceLineReader reader = new ResourceLineReader(resource);
		reader.read();
		String line = (String) reader.read();
		assertEquals("3", line);		
	}

	@org.junit.Test
public void testComments() throws Exception {
		Resource resource = new ByteArrayResource("1\n-- 2\n3".getBytes());
		ResourceLineReader reader = new ResourceLineReader(resource);
		reader.setComments(new String[] {"//", "--"});
		reader.read();
		String line = (String) reader.read();
		assertEquals("3", line);		
	}

    @org.junit.Test
public void testCommentOnTheLastLine() throws Exception {
        Resource resource = new ByteArrayResource("1\n#last line".getBytes());
        ResourceLineReader reader = new ResourceLineReader(resource);
        reader.read();
        String line = (String) reader.read();
        assertNull(line);        
    }
	
	@org.junit.Test
public void testResetNewReader() throws Exception {
		Resource resource = new ByteArrayResource("1\n4\n5".getBytes());
		ResourceLineReader reader = new ResourceLineReader(resource);
		reader.reset();
		assertEquals(0, reader.getPosition());
	}
	
	@org.junit.Test
public void testMarkReset() throws Exception {
		Resource resource = new ByteArrayResource("1\n4\n5".getBytes());
		ResourceLineReader reader = new ResourceLineReader(resource);
		reader.read();	
		assertEquals(1, reader.getPosition());
		reader.mark();
		reader.read();	
		assertEquals(2, reader.getPosition());
		reader.reset();
		reader.read();	
		assertEquals(2, reader.getPosition());
	}

	@org.junit.Test
public void testMarkOnFirstRead() throws Exception {
		Resource resource = new ByteArrayResource("1\n# 2\n3".getBytes());
		ResourceLineReader reader = new ResourceLineReader(resource);
		reader.read();
		// The first read should do a mark() so the reset goes back to the beginning.
		reader.reset();
		String line = (String) reader.read();
		assertEquals("1", line);		
	}

	@org.junit.Test
public void testMarkAfterClose() throws Exception {
		Resource resource = new ByteArrayResource("1\n# 2\n3".getBytes());
		ResourceLineReader reader = new ResourceLineReader(resource);
		reader.read();
		reader.close();
		reader.mark();		
	}

	@org.junit.Test
public void testNonDefaultRecordSeparatorPolicy() throws Exception {
		Resource resource = new ByteArrayResource("1\n\"4\n5\"; \n6".getBytes());
		ResourceLineReader reader = new ResourceLineReader(resource);
		reader.setRecordSeparatorPolicy(new SuffixRecordSeparatorPolicy());
		assertEquals(0, reader.getPosition());
		String line = (String) reader.read();
		assertEquals("1\"4\n5\"", line);				
	}
}
