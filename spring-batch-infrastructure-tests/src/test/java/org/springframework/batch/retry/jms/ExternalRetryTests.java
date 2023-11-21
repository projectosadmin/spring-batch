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

package org.springframework.batch.retry.jms;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.item.ItemRecoverer;
import org.springframework.batch.item.support.AbstractItemReader;
import org.springframework.batch.item.support.AbstractItemWriter;
import org.springframework.batch.jms.ExternalRetryInBatchTests;
import org.springframework.batch.retry.RecoveryCallback;
import org.springframework.batch.retry.RetryCallback;
import org.springframework.batch.retry.RetryContext;
import org.springframework.batch.retry.callback.RecoveryRetryCallback;
import org.springframework.batch.retry.policy.RecoveryCallbackRetryPolicy;
import org.springframework.batch.retry.support.RetryTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jms.core.JmsTemplate;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ClassUtils;

public class ExternalRetryTests extends AbstractDependencyInjectionSpringContextTests {

	private JmsTemplate jmsTemplate;

	private RetryTemplate retryTemplate;

	private ItemReaderRecoverer provider;

	private JdbcTemplate jdbcTemplate;

	private PlatformTransactionManager transactionManager;

	public void setDataSource(DataSource dataSource) {
		jdbcTemplate = new JdbcTemplate(dataSource);
	}

	public void setTransactionManager(PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	public void setJmsTemplate(JmsTemplate jmsTemplate) {
		this.jmsTemplate = jmsTemplate;
	}

	protected String[] getConfigLocations() {
		return new String[] { ClassUtils.addResourcePathToPackagePath(ExternalRetryInBatchTests.class,
				"jms-context.xml") };
	}

	@org.junit.Before
public void onSetUp() throws Exception {
		super.onSetUp();
		getMessages(); // drain queue
		jdbcTemplate.execute("delete from T_FOOS");
		jmsTemplate.convertAndSend("queue", "foo");
		provider = new ItemReaderRecoverer() {
			public Object read() {
				String text = (String) jmsTemplate.receiveAndConvert("queue");
				list.add(text);
				return text;
			}

			public Object recover(Object data, Throwable cause) {
				recovered.add(data);
				return data;
			}
		};
		retryTemplate = new RetryTemplate();
	}

	private void assertInitialState() {
		int count = jdbcTemplate.queryForObject("select count(*) from T_FOOS", Integer.class);
		assertEquals(0, count);
	}

	private List list = new ArrayList();

	private List recovered = new ArrayList();

	/**
	 * Message processing is successful on the second attempt but must receive
	 * the message again.
	 * 
	 * @throws Exception Exception
	 */
	@org.junit.Test
public void testExternalRetrySuccessOnSecondAttempt() throws Exception {

		assertInitialState();

		retryTemplate.setRetryPolicy(new RecoveryCallbackRetryPolicy());

		final AbstractItemWriter writer = new AbstractItemWriter() {
			public void write(final Object text) {
				jdbcTemplate.update("INSERT into T_FOOS (id,name,foo_date) values (?,?,null)", new Object[] {
						new Integer(list.size()), text });
				if (list.size() == 1) {
					throw new RuntimeException("Rollback!");
				}

			}
		};

		try {
			new TransactionTemplate(transactionManager).execute(new TransactionCallback() {
				public Object doInTransaction(TransactionStatus status) {
					try {
						final Object item = provider.read();
						RecoveryRetryCallback callback = new RecoveryRetryCallback(item, new RetryCallback() {
							public Object doWithRetry(RetryContext context) throws Throwable {
								writer.write(item);
								return null;
							}
						});
						return retryTemplate.execute(callback);
					}
					catch (Exception e) {
						throw new RuntimeException(e.getMessage(), e);
					}
				}
			});
			fail("Expected Exception");
		}
		catch (Exception e) {

			assertEquals("Rollback!", e.getMessage());

			// Client of retry template has to take care of rollback. This would
			// be a message listener container in the MDP case.

		}

		new TransactionTemplate(transactionManager).execute(new TransactionCallback() {
			public Object doInTransaction(TransactionStatus status) {
				try {
					final Object item = provider.read();
					RecoveryRetryCallback callback = new RecoveryRetryCallback(item, new RetryCallback() {
						public Object doWithRetry(RetryContext context) throws Throwable {
							writer.write(item);
							return null;
						}
					});
					return retryTemplate.execute(callback);
				}
				catch (Exception e) {
					throw new RuntimeException(e.getMessage(), e);
				}
			}
		});

		List msgs = getMessages();

		// The database portion committed once...
		int count = jdbcTemplate.queryForObject("select count(*) from T_FOOS", Integer.class);
		assertEquals(1, count);

		// ... and so did the message session.
		assertEquals("[]", msgs.toString());
	}

	/**
	 * Message processing fails on both attempts.
	 * 
	 * @throws Exception Exception
	 */
	@org.junit.Test
public void testExternalRetryWithRecovery() throws Exception {

		assertInitialState();

		retryTemplate.setRetryPolicy(new RecoveryCallbackRetryPolicy());

		final Object item = provider.read();
		final RecoveryRetryCallback callback = new RecoveryRetryCallback(item, new RetryCallback() {
			public Object doWithRetry(RetryContext context) throws Throwable {
				jdbcTemplate.update("INSERT into T_FOOS (id,name,foo_date) values (?,?,null)", new Object[] {
						new Integer(list.size()), item });
				throw new RuntimeException("Rollback!");
			}
		});
		
		callback.setRecoveryCallback(new RecoveryCallback() {
			public Object recover(RetryContext context) {
				return provider.recover(item, context.getLastThrowable());
			}
		});

		Object result = "start";

		for (int i = 0; i < 4; i++) {
			try {
				result = new TransactionTemplate(transactionManager).execute(new TransactionCallback() {
					public Object doInTransaction(TransactionStatus status) {
						try {
							return retryTemplate.execute(callback);
						}
						catch (Exception e) {
							throw new RuntimeException(e.getMessage(), e);
						}
					}
				});
			}
			catch (Exception e) {

				if (i < 3)
					assertEquals("Rollback!", e.getMessage());

				// Client of retry template has to take care of rollback. This
				// would
				// be a message listener container in the MDP case.

			}
		}

		// Last attempt should return last item.
		assertEquals("foo", result);

		List msgs = getMessages();

		assertEquals(1, recovered.size());

		// The database portion committed once...
		int count = jdbcTemplate.queryForObject("select count(*) from T_FOOS", Integer.class);
		assertEquals(0, count);

		// ... and so did the message session.
		assertEquals("[]", msgs.toString());

	}

	private List getMessages() {
		String next = "";
		List msgs = new ArrayList();
		while (next != null) {
			next = (String) jmsTemplate.receiveAndConvert("queue");
			if (next != null)
				msgs.add(next);
		}
		return msgs;
	}

	private abstract class ItemReaderRecoverer extends AbstractItemReader implements ItemRecoverer {

	}

}
