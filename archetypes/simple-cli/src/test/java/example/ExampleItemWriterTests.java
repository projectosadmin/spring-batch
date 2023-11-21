package example;

import static org.junit.Assert.*;

public class ExampleItemWriterTests {

	private ExampleItemWriter writer = new ExampleItemWriter();
	
	@org.junit.Test
public void testWrite() throws Exception {
		writer.write(null); // nothing bad happens
	}

}
