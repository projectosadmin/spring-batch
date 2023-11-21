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

package org.springframework.batch.support.transaction;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

public class TransactionAwareListFactoryTests {

	TransactionAwareProxyFactory factory = new TransactionAwareProxyFactory(Arrays.asList(new String[] { "foo",
			"bar", "spam" }));

	TransactionTemplate transactionTemplate = new TransactionTemplate(new ResourcelessTransactionManager());

	List list;

	    @org.junit.Before
public void setUp() throws Exception {
		list = (List) factory.createInstance();
	}

	@org.junit.Test
public void testAdd() {
		assertEquals(3, list.size());
		list.add("bucket");
		assertTrue(list.contains("bucket"));
	}

	@org.junit.Test
public void testRemove() {
		assertEquals(3, list.size());
		assertTrue(list.contains("spam"));
		list.remove("spam");
		assertFalse(list.contains("spam"));
	}

	@org.junit.Test
public void testClear() {
		assertEquals(3, list.size());
		list.clear();
		assertEquals(0, list.size());
	}

	@org.junit.Test
public void testTransactionalAdd() throws Exception {
		transactionTemplate.execute(new TransactionCallback() {
			public Object doInTransaction(TransactionStatus status) {
				testAdd();
				return null;
			}
		});
		assertEquals(4, list.size());
	}

	@org.junit.Test
public void testTransactionalRemove() throws Exception {
		transactionTemplate.execute(new TransactionCallback() {
			public Object doInTransaction(TransactionStatus status) {
				testRemove();
				return null;
			}
		});
		assertEquals(2, list.size());
	}

	@org.junit.Test
public void testTransactionalClear() throws Exception {
		transactionTemplate.execute(new TransactionCallback() {
			public Object doInTransaction(TransactionStatus status) {
				testClear();
				return null;
			}
		});
		assertEquals(0, list.size());
	}

	@org.junit.Test
public void testTransactionalAddWithRollback() throws Exception {
		try {
			transactionTemplate.execute(new TransactionCallback() {
				public Object doInTransaction(TransactionStatus status) {
					testAdd();
					throw new RuntimeException("Rollback!");
				}
			});
			fail("Expected RuntimeException");
		}
		catch (RuntimeException e) {
			assertEquals("Rollback!", e.getMessage());
		}
		assertEquals(3, list.size());
	}

	@org.junit.Test
public void testTransactionalRemoveWithRollback() throws Exception {
		try {
			transactionTemplate.execute(new TransactionCallback() {
				public Object doInTransaction(TransactionStatus status) {
					testRemove();
					throw new RuntimeException("Rollback!");
				}
			});
			fail("Expected RuntimeException");
		}
		catch (RuntimeException e) {
			assertEquals("Rollback!", e.getMessage());
		}
		assertEquals(3, list.size());
	}

	@org.junit.Test
public void testTransactionalClearWithRollback() throws Exception {
		try {
			transactionTemplate.execute(new TransactionCallback() {
				public Object doInTransaction(TransactionStatus status) {
					testClear();
					throw new RuntimeException("Rollback!");
				}
			});
			fail("Expected RuntimeException");
		}
		catch (RuntimeException e) {
			assertEquals("Rollback!", e.getMessage());
		}
		assertEquals(3, list.size());
	}
}
