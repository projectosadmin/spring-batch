/*
 * Copyright 2006-2008 the original author or authors.
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
package org.springframework.batch.core.step.item;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.item.file.FlatFileParseException;

/**
 * @author Lucas Ward
 *
 */
public class SkipLimitReadFailurePolicyTests {

	private LimitCheckingItemSkipPolicy failurePolicy;
	
	    @org.junit.Before
public void setUp() throws Exception {
		
		
		List skippableExceptions = new ArrayList();
		skippableExceptions.add(FlatFileParseException.class);
		List fatalExceptions = Collections.EMPTY_LIST;
		
		failurePolicy = new LimitCheckingItemSkipPolicy(1, skippableExceptions, fatalExceptions);
	}
	
	@org.junit.Test
public void testLimitExceed(){
		try{
			failurePolicy.shouldSkip(new FlatFileParseException("", ""), 2);
			fail();
		}
		catch(SkipLimitExceededException ex){
			//expected
		}
	}
	
	@org.junit.Test
public void testNonSkippableException(){
		assertFalse(failurePolicy.shouldSkip(new FileNotFoundException(), 2));
	}
	
	@org.junit.Test
public void testSkip(){
		assertTrue(failurePolicy.shouldSkip(new FlatFileParseException("",""), 0));
	}

}
