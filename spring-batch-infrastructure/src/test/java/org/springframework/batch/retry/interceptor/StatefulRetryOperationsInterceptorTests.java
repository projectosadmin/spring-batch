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
package org.springframework.batch.retry.interceptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.target.SingletonTargetSource;
import org.springframework.batch.item.ItemRecoverer;
import org.springframework.batch.retry.ExhaustedRetryException;
import org.springframework.batch.retry.policy.AlwaysRetryPolicy;
import org.springframework.batch.retry.policy.NeverRetryPolicy;
import org.springframework.batch.retry.policy.SimpleRetryPolicy;

/**
 * @author Dave Syer
 * 
 */
public class StatefulRetryOperationsInterceptorTests {

	private StatefulRetryOperationsInterceptor interceptor;

	private Service service;

	private Transformer transformer;

	private static int count;

	    @org.junit.Before
public void setUp() throws Exception {
		interceptor = new StatefulRetryOperationsInterceptor();
		service = (Service) ProxyFactory.getProxy(Service.class, new SingletonTargetSource(new ServiceImpl()));
		transformer = (Transformer) ProxyFactory.getProxy(Transformer.class, new SingletonTargetSource(new TransformerImpl()));
		count = 0;
	}

	@org.junit.Test
public void testDefaultInterceptorSunnyDay() throws Exception {
		((Advised) service).addAdvice(interceptor);
		try {
			service.service("foo");
			fail("Expected Exception.");
		}
		catch (Exception e) {
			String message = e.getMessage();
			assertTrue("Wrong message: " + message, message.startsWith("Not enough calls"));
		}
		assertEquals(1, count);
	}

	@org.junit.Test
public void testDefaultTransformerInterceptorSunnyDay() throws Exception {
		((Advised) transformer).addAdvice(interceptor);
		try {
			transformer.transform("foo");
			fail("Expected Exception.");
		}
		catch (Exception e) {
			String message = e.getMessage();
			assertTrue("Wrong message: " + message, message.startsWith("Not enough calls"));
		}
		assertEquals(1, count);
	}

	@org.junit.Test
public void testDefaultInterceptorAlwaysRetry() throws Exception {
		interceptor.setRetryPolicy(new AlwaysRetryPolicy());
		((Advised) service).addAdvice(interceptor);
		try {
			service.service("foo");
			fail("Expected Exception.");
		}
		catch (Exception e) {
			String message = e.getMessage();
			assertTrue("Wrong message: " + message, message.startsWith("Not enough calls"));
		}
		assertEquals(1, count);
	}

	@org.junit.Test
public void testInterceptorChainWithRetry() throws Exception {
		((Advised) service).addAdvice(interceptor);
		final List list = new ArrayList();
		((Advised) service).addAdvice(new MethodInterceptor() {
			public Object invoke(MethodInvocation invocation) throws Throwable {
				list.add("chain");
				return invocation.proceed();
			}
		});
		interceptor.setRetryPolicy(new SimpleRetryPolicy(2));
		try {
			service.service("foo");
			fail("Expected Exception.");
		}
		catch (Exception e) {
			String message = e.getMessage();
			assertTrue("Wrong message: " + message, message.startsWith("Not enough calls"));
		}
		assertEquals(1, count);
		service.service("foo");
		assertEquals(2, count);
		assertEquals(2, list.size());
	}

	@org.junit.Test
public void testTransformerWithSuccessfulRetry() throws Exception {
		((Advised) transformer).addAdvice(interceptor);
		interceptor.setRetryPolicy(new SimpleRetryPolicy(2));
		try {
			transformer.transform("foo");
			fail("Expected Exception.");
		}
		catch (Exception e) {
			String message = e.getMessage();
			assertTrue("Wrong message: " + message, message.startsWith("Not enough calls"));
		}
		assertEquals(1, count);
		Collection result = transformer.transform("foo");
		assertEquals(2, count);
		assertEquals(1, result.size());
	}

	@org.junit.Test
public void testRetryExceptionAfterTooManyAttemptsWithNoRecovery() throws Exception {
		((Advised) service).addAdvice(interceptor);
		interceptor.setRetryPolicy(new NeverRetryPolicy());
		try {
			service.service("foo");
			fail("Expected Exception.");
		}
		catch (Exception e) {
			String message = e.getMessage();
			assertTrue("Wrong message: " + message, message.startsWith("Not enough calls"));
		}
		assertEquals(1, count);
		try {
			service.service("foo");
			fail("Expected ExhaustedRetryException");
		} catch (ExhaustedRetryException e) {
			// expected
			String message = e.getMessage();
			assertTrue("Wrong message: " + message, message.startsWith("Retry was exhausted but there was no recover"));
		}
		assertEquals(1, count);
	}

	@org.junit.Test
public void testRecoveryAfterTooManyAttempts() throws Exception {
		((Advised) service).addAdvice(interceptor);
		interceptor.setRetryPolicy(new NeverRetryPolicy());
		try {
			service.service("foo");
			fail("Expected Exception.");
		}
		catch (Exception e) {
			String message = e.getMessage();
			assertTrue("Wrong message: " + message, message.startsWith("Not enough calls"));
		}
		assertEquals(1, count);
		interceptor.setRecoverer(new ItemRecoverer() {
			public Object recover(Object data, Throwable cause) {
				count++;
				return data;
			}
		});
		service.service("foo");
		assertEquals(2, count);
	}

	@org.junit.Test
public void testTransformerRecoveryAfterTooManyAttempts() throws Exception {
		((Advised) transformer).addAdvice(interceptor);
		interceptor.setRetryPolicy(new NeverRetryPolicy());
		try {
			transformer.transform("foo");
			fail("Expected Exception.");
		}
		catch (Exception e) {
			String message = e.getMessage();
			assertTrue("Wrong message: " + message, message.startsWith("Not enough calls"));
		}
		assertEquals(1, count);
		interceptor.setRecoverer(new ItemRecoverer() {
			public Object recover(Object data, Throwable cause) {
				count++;
				return Collections.singleton(data);
			}
		});
		Collection result = transformer.transform("foo");
		assertEquals(2, count);
		assertEquals(1, result.size());
	}

	public static interface Service {
		void service(String in) throws Exception;
	}

	public static class ServiceImpl implements Service {

		public void service(String in) throws Exception {
			count++;
			if (count < 2) {
				throw new Exception("Not enough calls: " + count);
			}
		}

	}

	public static interface Transformer {
		Collection transform(String in) throws Exception;
	}

	public static class TransformerImpl implements Transformer {

		public Collection transform(String in) throws Exception {
			count++;
			if (count < 2) {
				throw new Exception("Not enough calls: " + count);
			}
			return Collections.singleton(in + ":" + count);
		}

	}
}
