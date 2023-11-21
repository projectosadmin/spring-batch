package org.springframework.batch.sample.validation.valang.custom;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.easymock.MockControl;

import org.springframework.batch.sample.domain.LineItem;

import org.springmodules.validation.valang.functions.Function;
import static org.junit.Assert.*;

public class ValidateHandlingPricesFunctionTests {

	private ValidateHandlingPricesFunction function;
	private MockControl argumentControl;
	private Function argument;

	    @org.junit.Before
public void setUp() {
		argumentControl = MockControl.createControl(Function.class);
		argument = (Function) argumentControl.getMock();

		//create function
		function = new ValidateHandlingPricesFunction(new Function[] {argument}, 0, 0);
	}
	
	@org.junit.Test
public void testHandlingPriceMin() throws Exception {
	
		//create line item with correct handling price
		LineItem item = new LineItem();
		item.setHandlingPrice(new BigDecimal(1.0));
		
		//add it to line items list 
		List items = new ArrayList();
		items.add(item);
		
		//set return value for mock argument
		argument.getResult(null);
		argumentControl.setReturnValue(items,2);
		argumentControl.replay();
		
		//verify result - should be true - all handling prices are correct
		assertTrue(((Boolean)function.doGetResult(null)).booleanValue());
		
		//now add line item with negative handling price
		item = new LineItem();
		item.setHandlingPrice(new BigDecimal(-1.0));
		items.add(item);
		
		//verify result - should be false - second item has invalid handling price
		assertFalse(((Boolean)function.doGetResult(null)).booleanValue());
	}
	
	@org.junit.Test
public void testHandlingPriceMax() throws Exception {

		//create line item with correct handling price
		LineItem item = new LineItem();
		item.setHandlingPrice(new BigDecimal(99999999.0));
		
		//add it to line items list 
		List items = new ArrayList();
		items.add(item);
		
		//set return value for mock argument
		argument.getResult(null);
		argumentControl.setReturnValue(items,2);
		argumentControl.replay();
		
		//verify result - should be true - all handling prices are correct
		assertTrue(((Boolean)function.doGetResult(null)).booleanValue());
		
		//now add line item with handling price above allowed max 
		item = new LineItem();
		item.setHandlingPrice(new BigDecimal(100000000.0));
		items.add(item);
		
		//verify result - should be false - second item has invalid handling price
		assertFalse(((Boolean)function.doGetResult(null)).booleanValue());
	}
}
