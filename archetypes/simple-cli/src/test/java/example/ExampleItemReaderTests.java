package example;

import static org.junit.Assert.*;

public class ExampleItemReaderTests {

	private ExampleItemReader reader = new ExampleItemReader();
	
	@org.junit.Test
public void testReadOnce() throws Exception {
		assertEquals("Hello world!", reader.read());
	}

	@org.junit.Test
public void testReadTwice() throws Exception {
		reader.read();
		assertEquals(null, reader.read());
	}

}
