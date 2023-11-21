package org.springframework.batch.sample.validation.valang.custom;

import java.util.Date;

import org.easymock.MockControl;
import org.springmodules.validation.valang.functions.Function;
import static org.junit.Assert.*;

public class FutureDateFunctionTests {

	private FutureDateFunction function;
	private MockControl argumentControl;
	private Function argument;
	
	    @org.junit.Before
public void setUp() {
		argumentControl = MockControl.createControl(Function.class);
		argument = (Function) argumentControl.getMock();

		//create function
		function = new FutureDateFunction(new Function[] {argument}, 0, 0);
	}
	
	@org.junit.Test
public void testFunctionWithNonDateValue() {
		
		//set-up mock argument - set return value to non Date value
		argument.getResult(null);
		argumentControl.setReturnValue(this);
		argumentControl.replay();
				
		//call tested method - exception is expected because non date value
		try {
			function.doGetResult(null);
			fail("Exception was expected.");
		} catch (Exception e) {
			assertTrue(true);
		}
	}
	
	@org.junit.Test
public void testFunctionWithFutureDate() throws Exception {

		//set-up mock argument - set return value to future Date
		argument.getResult(null);
		argumentControl.setReturnValue(new Date(Long.MAX_VALUE));
		argumentControl.replay();
		
		//vefify result - should be true because of future date
		assertTrue(((Boolean)function.doGetResult(null)).booleanValue());
	}
	
	@org.junit.Test
public void testFunctionWithPastDate() throws Exception {

		//set-up mock argument - set return value to future Date
		argument.getResult(null);
		argumentControl.setReturnValue(new Date(0));
		argumentControl.replay();
		
		//vefify result - should be false because of past date
		assertFalse(((Boolean)function.doGetResult(null)).booleanValue());
		
	}
}
