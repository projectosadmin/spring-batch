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

package org.springframework.batch.item.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

import org.junit.Before;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.batch.support.transaction.TransactionAwareProxyFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import static org.junit.Assert.*;
public class TransactionAwareListItemReaderTests {

	// TransactionAwareListItemProvider provider = new
	// TransactionAwareListItemProvider(Arrays.asList(new String[] { "a",
	// "b", "c" }));
	ListItemReader provider;
@org.junit.Before
public void setUp() throws Exception {
		//
		TransactionAwareProxyFactory factory = new TransactionAwareProxyFactory(Arrays.asList(new String[] { "a", "b",
				"c" }));
		provider = new ListItemReader((List) factory.createInstance());
	}

	@org.junit.Test
public void testNext() throws Exception {
		assertEquals("a", provider.read());
		assertEquals("b", provider.read());
		assertEquals("c", provider.read());
		assertEquals(null, provider.read());
	}

	@org.junit.Test
public void testCommit() throws Exception {
		PlatformTransactionManager transactionManager = new ResourcelessTransactionManager();
		final List taken = new ArrayList();
		try {
			new TransactionTemplate(transactionManager).execute(new TransactionCallback() {
				public Object doInTransaction(TransactionStatus status) {
					taken.add(provider.read());
					return null;
				}
			});
		}
		catch (RuntimeException e) {
			fail("Unexpected RuntimeException");
			assertEquals("Rollback!", e.getMessage());
		}
		assertEquals(1, taken.size());
		assertEquals("a", taken.get(0));
		taken.clear();
		Object next = provider.read();
		while (next != null) {
			taken.add(next);
			next = provider.read();
		}
		// System.err.println(taken);
		assertFalse(taken.contains("a"));
	}

	@org.junit.Test
public void testTransactionalExhausted() throws Exception {
		PlatformTransactionManager transactionManager = new ResourcelessTransactionManager();
		final List taken = new ArrayList();
		new TransactionTemplate(transactionManager).execute(new TransactionCallback() {
			public Object doInTransaction(TransactionStatus status) {
				Object next = provider.read();
				while (next != null) {
					taken.add(next);
					next = provider.read();
				}
				return null;
			}
		});
		assertEquals(3, taken.size());
		assertEquals("a", taken.get(0));
	}

	@org.junit.Test
public void testRollback() throws Exception {
		PlatformTransactionManager transactionManager = new ResourcelessTransactionManager();
		final List taken = new ArrayList();
		try {
			new TransactionTemplate(transactionManager).execute(new TransactionCallback() {
				public Object doInTransaction(TransactionStatus status) {
					taken.add(provider.read());
					throw new RuntimeException("Rollback!");
				}
			});
			fail("Expected RuntimeException");
		}
		catch (RuntimeException e) {
			assertEquals("Rollback!", e.getMessage());
		}
		assertEquals(1, taken.size());
		assertEquals("a", taken.get(0));
		taken.clear();
		Object next = provider.read();
		while (next != null) {
			taken.add(next);
			next = provider.read();
		}
		System.err.println(taken);
		assertTrue(taken.contains("a"));
	}

}
