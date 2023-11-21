package org.springframework.batch.item.xml.oxm;

import java.io.IOException;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;

import static org.junit.Assert.*;

import org.easymock.MockControl;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;
import org.springframework.oxm.Unmarshaller;

/**
 * Tests for {@link UnmarshallingEventReaderDeserializer}
 * 
 * @author Robert Kasanicky
 */
public class UnmarshallingFragmentDeserializerTests {

	// object under test
	private UnmarshallingEventReaderDeserializer deserializer;
	
	private XMLEventReader eventReader;
	private String xml = "<root> </root>";
	
	private Unmarshaller unmarshaller;
	private MockControl unmarshallerControl = MockControl.createStrictControl(Unmarshaller.class);
	
	

	    @org.junit.Before
public void setUp() throws Exception {
		Resource input = new ByteArrayResource(xml.getBytes());
		eventReader = XMLInputFactory.newInstance().createXMLEventReader(input.getInputStream());
		unmarshaller = (Unmarshaller) unmarshallerControl.getMock();
		unmarshallerControl.setDefaultMatcher(MockControl.ALWAYS_MATCHER);
		deserializer = new UnmarshallingEventReaderDeserializer(unmarshaller);
	}

	/**
	 * Regular scenario when deserializer returns the object provided by Unmarshaller
	 */
	@org.junit.Test
public void testSuccessfulDeserialization() throws Exception {
		Object expectedResult = new Object();
		unmarshaller.unmarshal(null);
		unmarshallerControl.setReturnValue(expectedResult);
		unmarshallerControl.replay();
		
		Object result = deserializer.deserializeFragment(eventReader);
		
		assertEquals(expectedResult, result);
		
		unmarshallerControl.verify();
	}
	
	/**
	 * Appropriate exception rethrown in case of failure.
	 */
	@org.junit.Test
public void testFailedDeserialization() throws Exception {
		unmarshaller.unmarshal(null);
		unmarshallerControl.setThrowable(new IOException());
		unmarshallerControl.replay();
		
		try {
			deserializer.deserializeFragment(eventReader);
			fail("Exception expected");
		}
		catch (DataAccessException e) {
			// expected
		}
		
		unmarshallerControl.verify();
	}
	
	/**
	 * It makes no sense to create UnmarshallingFragmentDeserializer with null Unmarshaller,
	 * therefore it should cause exception.
	 */
	@org.junit.Test
public void testExceptionOnNullUnmarshaller() {
		try {
			deserializer = new UnmarshallingEventReaderDeserializer(null);
			fail("Exception expected");
		}
		catch (IllegalArgumentException e) {
			// expected
		}
		
	}
}
