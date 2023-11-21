/*
 * Copyright 2006-2007 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.item.validator;

import static org.junit.Assert.*;

import org.easymock.MockControl;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.AbstractItemReader;

/**
 * @author Lucas Ward
 *
 */
public class ValidatingItemReaderTests {

	ItemReader inputSource;
	ValidatingItemReader itemProvider;
	Validator validator;
	MockControl validatorControl = MockControl.createControl(Validator.class);

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	    @org.junit.Before
public void setUp() throws Exception {
		

		inputSource = new MockItemReader(this);
		validator = (Validator)validatorControl.getMock();
		itemProvider = new ValidatingItemReader();
		itemProvider.setItemReader(inputSource);
		itemProvider.setValidator(validator);
	}

	/*
	 * Super class' afterPropertieSet should be called to
	 * ensure ItemReader is set.
	 */
	@org.junit.Test
public void testItemReaderPropertiesSet(){
		try{
			itemProvider.setItemReader(null);
			itemProvider.afterPropertiesSet();
			fail();
		}catch(Exception ex){
			assertTrue(ex instanceof IllegalArgumentException);
		}
	}

	@org.junit.Test
public void testValidatorPropertesSet(){
		try{
			itemProvider.setValidator(null);
			itemProvider.afterPropertiesSet();
			fail();
		}catch(Exception ex){
			assertTrue(ex instanceof IllegalArgumentException);
		}
	}

	@org.junit.Test
public void testValidation() throws Exception{

		validator.validate(this);
		validatorControl.replay();
		assertEquals(itemProvider.read(), this);
		validatorControl.verify();
	}

	@org.junit.Test
public void testValidationException() throws Exception{

		validator.validate(this);
		validatorControl.setThrowable(new ValidationException(""));
		validatorControl.replay();
		try{
			itemProvider.read();
			fail();
		}catch(ValidationException ex){
			//expected
		}
	}

	@org.junit.Test
public void testNullInput() throws Exception{
		validatorControl.replay();
		itemProvider.setItemReader(new MockItemReader(null));
		assertNull(itemProvider.read());
		//assert validator wasn't called.
		validatorControl.verify();
	}

	private static class MockItemReader extends AbstractItemReader {

		Object value;

		public MockItemReader(Object value){
			this.value = value;
		}

		public Object read() {
			return value;
		}
	}
}
