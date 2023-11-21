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
package org.springframework.batch.core.step.item;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.transaction.interceptor.TransactionInterceptor;

import static org.junit.Assert.*;

/**
 * @author Dave Syer
 *
 */
public class TransactionInterceptorValidatorTests {
	
	private TransactionInterceptorValidator validator = new TransactionInterceptorValidator(1);

	@org.junit.Test
public void testValidateNull() {
		try {
			validator.validate(null);
			fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			String message = e.getMessage();
			assertTrue("Wrong message: "+message, message.indexOf("JobRepository")>=0);
		}
	}

	@org.junit.Test
public void testValidateWithNoInterceptors() {
		validator.validate(new Object());
	}
	
	@org.junit.Test
public void testValidateAdvisedWithOneInterceptor() {
		validator.validate(ProxyFactory.getProxy(JobRepository.class, new TransactionInterceptor()));
	}
	
	@org.junit.Test
public void testValidateAdvisedWithTwoInterceptors() {
		Object target = ProxyFactory.getProxy(JobRepository.class, new TransactionInterceptor());
		ProxyFactory factory = new ProxyFactory();
		factory.setTarget(target);
		factory.addInterface(JobRepository.class);
		factory.addAdvice(new TransactionInterceptor());
		try {
			validator.validate(factory.getProxy());
			fail("Expected IllegalStateException");
		} catch (IllegalStateException e) {
			String message = e.getMessage();
			assertTrue("Wrong message: "+message, message.indexOf("JobRepository")>=0);
		}
	}
	
}
