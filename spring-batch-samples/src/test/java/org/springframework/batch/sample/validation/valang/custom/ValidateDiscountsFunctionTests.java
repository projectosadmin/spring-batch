package org.springframework.batch.sample.validation.valang.custom;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.sample.domain.LineItem;

import org.easymock.MockControl;
import org.springmodules.validation.valang.functions.Function;
import static org.junit.Assert.*;

public class ValidateDiscountsFunctionTests {

	private ValidateDiscountsFunction function;
	private MockControl argumentControl;
	private Function argument;

	    @org.junit.Before
public void setUp() {
		argumentControl = MockControl.createControl(Function.class);
		argument = (Function) argumentControl.getMock();

		//create function
		function = new ValidateDiscountsFunction(new Function[] {argument}, 0, 0);
	}
	
	@org.junit.Test
public void testDiscountPercentageMin() throws Exception {
	
		//create line item with correct discount percentage and zero discount amount
		LineItem item = new LineItem();
		item.setDiscountPerc(new BigDecimal(1.0));
		item.setDiscountAmount(new BigDecimal(0.0));
		
		//add it to line items list 
		List items = new ArrayList();
		items.add(item);
		
		//set return value for mock argument
		argument.getResult(null);
		argumentControl.setReturnValue(items,2);
		argumentControl.replay();
		
		//verify result - should be true - all discount percentages are correct
		assertTrue(((Boolean)function.doGetResult(null)).booleanValue());
		
		//now add line item with negative percentage
		item = new LineItem();
		item.setDiscountPerc(new BigDecimal(-1.0));
		item.setDiscountAmount(new BigDecimal(0.0));
		items.add(item);
		
		//verify result - should be false - second item has invalid discount percentage
		assertFalse(((Boolean)function.doGetResult(null)).booleanValue());
	}
	
	@org.junit.Test
public void testDiscountPercentageMax() throws Exception {

		//create line item with correct discount percentage and zero discount amount
		LineItem item = new LineItem();
		item.setDiscountPerc(new BigDecimal(99.0));
		item.setDiscountAmount(new BigDecimal(0.0));
		
		//add it to line items list 
		List items = new ArrayList();
		items.add(item);
		
		//set return value for mock argument
		argument.getResult(null);
		argumentControl.setReturnValue(items,2);
		argumentControl.replay();
		
		//verify result - should be true - all discount percentages are correct
		assertTrue(((Boolean)function.doGetResult(null)).booleanValue());
		
		//now add line item with discount percentage above 100
		item = new LineItem();
		item.setDiscountPerc(new BigDecimal(101.0));
		item.setDiscountAmount(new BigDecimal(0.0));
		items.add(item);
		
		//verify result - should be false - second item has invalid discount percentage
		assertFalse(((Boolean)function.doGetResult(null)).booleanValue());
	}
	
	@org.junit.Test
public void testDiscountPriceMin() throws Exception {

		//create line item with correct discount amount and zero discount percentage
		LineItem item = new LineItem();
		item.setDiscountPerc(new BigDecimal(0.0));
		item.setDiscountAmount(new BigDecimal(10.0));
		item.setPrice(new BigDecimal(100.0));
		
		//add it to line items list 
		List items = new ArrayList();
		items.add(item);
		
		//set return value for mock argument
		argument.getResult(null);
		argumentControl.setReturnValue(items,2);
		argumentControl.replay();
		
		//verify result - should be true - all discount amounts are correct
		assertTrue(((Boolean)function.doGetResult(null)).booleanValue());
		
		//now add line item with negative discount amount
		item = new LineItem();
		item.setDiscountPerc(new BigDecimal(0.0));
		item.setDiscountAmount(new BigDecimal(-1.0));
		item.setPrice(new BigDecimal(100.0));
		items.add(item);
		
		//verify result - should be false - second item has invalid discount amount
		assertFalse(((Boolean)function.doGetResult(null)).booleanValue());
	}
	
	@org.junit.Test
public void testDiscountPriceMax() throws Exception {

		//create line item with correct discount amount and zero discount percentage
		LineItem item = new LineItem();
		item.setDiscountPerc(new BigDecimal(0.0));
		item.setDiscountAmount(new BigDecimal(99.0));
		item.setPrice(new BigDecimal(100.0));
		
		//add it to line items list 
		List items = new ArrayList();
		items.add(item);
		
		//set return value for mock argument
		argument.getResult(null);
		argumentControl.setReturnValue(items,2);
		argumentControl.replay();
		
		//verify result - should be true - all discount amounts are correct
		assertTrue(((Boolean)function.doGetResult(null)).booleanValue());
		
		//now add line item with discount amount above item price
		item = new LineItem();
		item.setDiscountPerc(new BigDecimal(0.0));
		item.setDiscountAmount(new BigDecimal(101.0));
		item.setPrice(new BigDecimal(100.0));
		items.add(item);
		
		//verify result - should be false - second item has invalid discount amount
		assertFalse(((Boolean)function.doGetResult(null)).booleanValue());
	}
	
	@org.junit.Test
public void testBothDiscountValuesNonZero() throws Exception {

		//create line item with non-zero discount amount and non-zero discount percentage
		LineItem item = new LineItem();
		item.setDiscountPerc(new BigDecimal(10.0));
		item.setDiscountAmount(new BigDecimal(99.0));
		
		//add it to line items list 
		List items = new ArrayList();
		items.add(item);
		
		//set return value for mock argument
		argument.getResult(null);
		argumentControl.setReturnValue(items);
		argumentControl.replay();

		//verify result - should be false - only one of the discount values is empty
		assertFalse(((Boolean)function.doGetResult(null)).booleanValue());
	}
}
