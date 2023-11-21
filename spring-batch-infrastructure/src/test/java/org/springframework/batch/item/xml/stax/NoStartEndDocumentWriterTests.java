package org.springframework.batch.item.xml.stax;

import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.events.XMLEvent;

import static org.junit.Assert.*;

import org.easymock.MockControl;

/**
 * Tests for {@link NoStartEndDocumentStreamWriter}
 * 
 * @author Robert Kasanicky
 */
public class NoStartEndDocumentWriterTests {

	// object under test
	private NoStartEndDocumentStreamWriter writer;
	
	private XMLEventWriter wrappedWriter;
	private MockControl wrappedWriterControl = MockControl.createStrictControl(XMLEventWriter.class);
	
	private XMLEventFactory eventFactory = XMLEventFactory.newInstance();
	
	
	    @org.junit.Before
public void setUp() throws Exception {
		wrappedWriter = (XMLEventWriter) wrappedWriterControl.getMock();
		writer = new NoStartEndDocumentStreamWriter(wrappedWriter);
	}


	/**
	 * StartDocument and EndDocument events are not passed to the wrapped writer.
	 */
	@org.junit.Test
public void testNoStartEnd() throws Exception {
		XMLEvent event = eventFactory.createComment("testEvent");
		
		//mock expects only a single event
		wrappedWriter.add(event);
		wrappedWriterControl.setVoidCallable();
		wrappedWriterControl.replay();
		
		writer.add(eventFactory.createStartDocument());
		writer.add(event);
		writer.add(eventFactory.createEndDocument());
		
		wrappedWriterControl.verify();
	}
}
