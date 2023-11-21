package org.springframework.batch.item.xml.stax;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;

import static org.junit.Assert.*;

import org.springframework.batch.item.xml.EventHelper;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

/**
 * Tests for {@link DefaultTransactionalEventReader}.
 * 
 * @deprecated no longer used, to be removed in 2.0
 * 
 * @author Robert Kasanicky
 */
public class DefaultTransactionalEventReaderTests {

	// object under test
	private TransactionalEventReader reader;

	// test xml input
	private String xml = "<root> <fragment> <misc1/> </fragment> <misc2/> <fragment> </fragment> </root>";

	
	    @org.junit.Before
public void setUp() throws Exception {
		Resource resource = new ByteArrayResource(xml.getBytes());
		XMLEventReader wrappedReader = XMLInputFactory.newInstance().createXMLEventReader(resource.getInputStream());
		reader = new DefaultTransactionalEventReader(wrappedReader);
	}

	/**
	 * Rollback scenario.
	 */
	@org.junit.Test
public void testRollback() throws Exception {
		assertTrue(reader.hasNext());
		reader.nextEvent(); //start document
		reader.nextEvent(); //start root element
		reader.nextEvent(); //whitespace
		
		reader.onCommit(); // commit point
		
		assertTrue(EventHelper.startElementName(reader.nextEvent()).equals("fragment"));
		reader.nextEvent(); //whitespace
		assertTrue(EventHelper.startElementName(reader.nextEvent()).equals("misc1"));
		assertTrue(EventHelper.endElementName(reader.peek()).equals("misc1"));
		
		reader.onRollback(); // now we should be at the last commit point
		assertTrue(reader.hasNext());
		assertTrue(EventHelper.startElementName(reader.nextEvent()).equals("fragment"));
		reader.nextEvent();
		assertTrue(EventHelper.startElementName(reader.nextEvent()).equals("misc1"));
	}
	
	/**
	 * Remove operation is not supported
	 */
	@org.junit.Test
public void testRemove() {
		try {
			reader.remove();
			fail("UnsupportedOperationException expected on calling remove()");
		}
		catch (UnsupportedOperationException e) {
			// expected
		}
	}
}
