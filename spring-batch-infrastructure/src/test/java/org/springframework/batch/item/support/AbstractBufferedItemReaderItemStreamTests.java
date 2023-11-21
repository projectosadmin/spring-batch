/**
 * 
 */
package org.springframework.batch.item.support;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Lucas Ward
 *
 */
public class AbstractBufferedItemReaderItemStreamTests {
	
	StringReader reader;
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	    @org.junit.Before
public void setUp() throws Exception {
		
		
		reader = new StringReader(Arrays.asList(new String[]{"a", "b", "c", "d", "e"}));
	}
	
	@org.junit.Test
public void testNormalUsage() throws Exception{
		
		assertEquals("a", reader.read());
		assertEquals("b", reader.read());
		assertEquals("c", reader.read());
	}
	
	@org.junit.Test
public void testMarkAndReset() throws Exception{
		
		reader.mark();
		assertEquals("a", reader.read());
		assertEquals("b", reader.read());
		reader.reset();
		assertEquals("a", reader.read());
	}
	
	@org.junit.Test
public void testReaderExceptionWithoutRollback() throws Exception{
		
		reader.mark();
		assertEquals("a", reader.read());
		assertEquals("b", reader.read());
		reader.setThrowException(true);
		try{
			reader.read();
			fail();
		}
		catch(RuntimeException ex){
			//expected
		}
		reader.setThrowException(false);
		reader.mark();
		assertEquals("c", reader.read());
		
		assertEquals("d", reader.read());
	}
	
	@org.junit.Test
public void testResetWithoutException() throws Exception{
		
		reader.mark();
		assertEquals("a", reader.read());
		assertEquals("b", reader.read());
		reader.reset();
		assertEquals("a", reader.read());
	}
	
	@org.junit.Test
public void testResetWithException() throws Exception{
		
		reader.mark();
		assertEquals("a", reader.read());
		assertEquals("b", reader.read());
		reader.setThrowException(true);
		try{
			reader.read();
			fail();
		}
		catch(RuntimeException ex){
			//expected
		}
		reader.setThrowException(false);
		reader.reset();
		assertEquals("a", reader.read());
	}
	
	
	private class StringReader extends AbstractBufferedItemReaderItemStream{

		final Iterator iterator;
		boolean throwException = false;
		
		public StringReader(List strings) {
			iterator = strings.iterator();
		}
		
		protected void doClose() throws Exception {
			
		}

		protected void doOpen() throws Exception {
			// TODO Auto-generated method stub
			
		}

		protected Object doRead() throws Exception {
			if(throwException){
				throw new RuntimeException();
			}
			else{
				return iterator.next();
			}
		}
		
		public void setThrowException(boolean throwException) {
			this.throwException = throwException;
		}
		
	}
}
