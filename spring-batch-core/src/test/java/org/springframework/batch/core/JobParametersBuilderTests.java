/**
 * 
 */
package org.springframework.batch.core;

import java.util.Date;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * @author Lucas Ward
 *
 */
public class JobParametersBuilderTests {

	JobParametersBuilder parametersBuilder = new JobParametersBuilder();
	
	Date date = new Date(System.currentTimeMillis());
	
	@org.junit.Test
public void testToJobRuntimeParamters(){
		parametersBuilder.addDate("SCHEDULE_DATE", date);
		parametersBuilder.addLong("LONG", new Long(1));
		parametersBuilder.addString("STRING", "string value");
		JobParameters parameters = parametersBuilder.toJobParameters();
		assertEquals(date, parameters.getDate("SCHEDULE_DATE"));
		assertEquals(new Long(1), parameters.getLong("LONG"));
		assertEquals("string value", parameters.getString("STRING"));
	}

	@org.junit.Test
public void testOrderedTypes(){
		parametersBuilder.addDate("SCHEDULE_DATE", date);
		parametersBuilder.addLong("LONG", new Long(1));
		parametersBuilder.addString("STRING", "string value");
		Iterator parameters = parametersBuilder.toJobParameters().getParameters().keySet().iterator();
		assertEquals("STRING", parameters.next());
		assertEquals("LONG", parameters.next());
		assertEquals("SCHEDULE_DATE", parameters.next());
	}

	@org.junit.Test
public void testOrderedStrings(){
		parametersBuilder.addString("foo", "value foo");
		parametersBuilder.addString("bar", "value bar");
		parametersBuilder.addString("spam", "value spam");
		Iterator parameters = parametersBuilder.toJobParameters().getParameters().keySet().iterator();
		assertEquals("foo", parameters.next());
		assertEquals("bar", parameters.next());
		assertEquals("spam", parameters.next());
	}
}
