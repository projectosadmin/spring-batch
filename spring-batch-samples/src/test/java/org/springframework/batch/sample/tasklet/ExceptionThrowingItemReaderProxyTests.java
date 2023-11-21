package org.springframework.batch.sample.tasklet;

import java.util.ArrayList;

import static org.junit.Assert.*;

import org.springframework.batch.core.UnexpectedJobExecutionException;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.context.RepeatContextSupport;
import org.springframework.batch.repeat.support.RepeatSynchronizationManager;
import org.springframework.batch.sample.item.reader.ExceptionThrowingItemReaderProxy;

public class ExceptionThrowingItemReaderProxyTests {

	//expected call count before exception is thrown (exception should be thrown in next iteration)
	private static final int ITER_COUNT = 5;
	
	@org.junit.After
    public void tearDown() throws Exception {
		RepeatSynchronizationManager.clear();
	}
	
	@org.junit.Test
public void testProcess() throws Exception {
				
		//create module and set item processor and iteration count
		ExceptionThrowingItemReaderProxy itemReader = new ExceptionThrowingItemReaderProxy();
		itemReader.setItemReader(new ListItemReader(new ArrayList() {{
			add("a");
			add("b");
			add("c");
			add("d");
			add("e");
			add("f");
		}}));

		itemReader.setThrowExceptionOnRecordNumber(ITER_COUNT + 1);
		
		RepeatSynchronizationManager.register(new RepeatContextSupport(null));
		
		//call process method multiple times and verify whether exception is thrown when expected
		for (int i = 0; i <= ITER_COUNT; i++) {
			try {
				itemReader.read();
				assertTrue(i < ITER_COUNT);
			} catch (UnexpectedJobExecutionException bce) {
				assertEquals(ITER_COUNT,i);
			}
		}
		
	}
}
