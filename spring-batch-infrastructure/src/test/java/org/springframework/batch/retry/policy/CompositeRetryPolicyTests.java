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

package org.springframework.batch.retry.policy;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

import org.springframework.batch.retry.RetryContext;
import org.springframework.batch.retry.RetryPolicy;
import org.springframework.batch.retry.TerminatedRetryException;

public class CompositeRetryPolicyTests {

	@org.junit.Test
public void testEmptyPolicies() throws Exception {
		CompositeRetryPolicy policy = new CompositeRetryPolicy();
		RetryContext context = policy.open(null, null);
		assertNotNull(context);
		assertTrue(policy.canRetry(context));
	}

	@org.junit.Test
public void testTrivialPolicies() throws Exception {
		CompositeRetryPolicy policy = new CompositeRetryPolicy();
		policy.setPolicies(new RetryPolicy[] { new MockRetryPolicySupport(), new MockRetryPolicySupport() });
		RetryContext context = policy.open(null, null);
		assertNotNull(context);
		assertTrue(policy.canRetry(context));
	}

	@org.junit.Test
public void testNonTrivialPolicies() throws Exception {
		CompositeRetryPolicy policy = new CompositeRetryPolicy();
		policy.setPolicies(new RetryPolicy[] { new MockRetryPolicySupport(), new MockRetryPolicySupport() {
			public boolean canRetry(RetryContext context) {
				return false;
			}
		} });
		RetryContext context = policy.open(null, null);
		assertNotNull(context);
		assertFalse(policy.canRetry(context));
	}

	@org.junit.Test
public void testNonTrivialPoliciesWithThrowable() throws Exception {
		CompositeRetryPolicy policy = new CompositeRetryPolicy();
		policy.setPolicies(new RetryPolicy[] { new MockRetryPolicySupport(), new MockRetryPolicySupport() {
			boolean errorRegistered = false;

			public boolean canRetry(RetryContext context) {
				return !errorRegistered;
			}

			public void registerThrowable(RetryContext context, Throwable throwable) throws TerminatedRetryException {
				errorRegistered = true;
			}
		} });
		RetryContext context = policy.open(null, null);
		assertNotNull(context);
		assertTrue(policy.canRetry(context));
		policy.registerThrowable(context, null);
		assertFalse(policy.canRetry(context));
	}

	@org.junit.Test
public void testNonTrivialPoliciesClose() throws Exception {
		final List list = new ArrayList();
		CompositeRetryPolicy policy = new CompositeRetryPolicy();
		policy.setPolicies(new RetryPolicy[] { new MockRetryPolicySupport() {
			public void close(RetryContext context) {
				list.add("1");
			}
		}, new MockRetryPolicySupport() {
			public void close(RetryContext context) {
				list.add("2");
			}
		} });
		RetryContext context = policy.open(null, null);
		assertNotNull(context);
		policy.close(context);
		assertEquals(2, list.size());
	}

	@org.junit.Test
public void testExceptionOnPoliciesClose() throws Exception {
		final List list = new ArrayList();
		CompositeRetryPolicy policy = new CompositeRetryPolicy();
		policy.setPolicies(new RetryPolicy[] { new MockRetryPolicySupport() {
			public void close(RetryContext context) {
				list.add("1");
				throw new RuntimeException("Pah!");
			}
		}, new MockRetryPolicySupport() {
			public void close(RetryContext context) {
				list.add("2");
			}
		} });
		RetryContext context = policy.open(null, null);
		assertNotNull(context);
		try {
			policy.close(context);
			fail("Expected RuntimeException");
		} catch (RuntimeException e) {
			assertEquals("Pah!", e.getMessage());
		}
		assertEquals(2, list.size());
	}

	@org.junit.Test
public void testRetryCount() throws Exception {
		CompositeRetryPolicy policy = new CompositeRetryPolicy();
		policy.setPolicies(new RetryPolicy[] { new MockRetryPolicySupport(), new MockRetryPolicySupport() });
		RetryContext context = policy.open(null, null);
		assertNotNull(context);
		policy.registerThrowable(context, null);
		assertEquals(0, context.getRetryCount());
		policy.registerThrowable(context, new RuntimeException("foo"));
		assertEquals(1, context.getRetryCount());
		assertEquals("foo", context.getLastThrowable().getMessage());
	}

	@org.junit.Test
public void testParent() throws Exception {
		CompositeRetryPolicy policy = new CompositeRetryPolicy();
		RetryContext context = policy.open(null, null);
		RetryContext child = policy.open(null, context);
		assertNotSame(child, context);
		assertSame(context, child.getParent());
	}

}
