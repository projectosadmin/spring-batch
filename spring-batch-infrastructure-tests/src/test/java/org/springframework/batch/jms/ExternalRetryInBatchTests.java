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

package org.springframework.batch.jms;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.item.ItemRecoverer;
import org.springframework.batch.item.support.AbstractItemReader;
import org.springframework.batch.repeat.ExitStatus;
import org.springframework.batch.repeat.RepeatCallback;
import org.springframework.batch.repeat.RepeatContext;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.batch.repeat.support.RepeatTemplate;
import org.springframework.batch.retry.RecoveryCallback;
import org.springframework.batch.retry.RetryCallback;
import org.springframework.batch.retry.RetryContext;
import org.springframework.batch.retry.callback.RecoveryRetryCallback;
import org.springframework.batch.retry.policy.RecoveryCallbackRetryPolicy;
import org.springframework.batch.retry.policy.SimpleRetryPolicy;
import org.springframework.batch.retry.support.RetryTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jms.core.JmsTemplate;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ClassUtils;

public class ExternalRetryInBatchTests extends AbstractDependencyInjectionSpringContextTests {
	private JmsTemplate jmsTemplate;

	private RetryTemplate retryTemplate;

	private RepeatTemplate repeatTemplate;

	private ItemReaderRecoverer provider;

	private JdbcTemplate jdbcTemplate;

	private PlatformTransactionManager transactionManager;

	public void setDataSource(DataSource dataSource) {
		jdbcTemplate = new JdbcTemplate(dataSource);
	}

	public void setTransactionManager(PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	public void setRepeatTemplate(RepeatTemplate repeatTemplate) {
		this.repeatTemplate = repeatTemplate;
	}

	public void setJmsTemplate(JmsTemplate jmsTemplate) {
		this.jmsTemplate = jmsTemplate;
	}

	protected String[] getConfigLocations() {
		return new String[] { ClassUtils.addResourcePathToPackagePath(getClass(), "jms-context.xml" )};
	}

	@org.junit.Before
public void onSetUp() throws Exception {
		super.onSetUp();
		getMessages(); // drain queue
		jdbcTemplate.execute("delete from T_FOOS");
		jmsTemplate.convertAndSend("queue", "foo");
		jmsTemplate.convertAndSend("queue", "bar");
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

	@org.junit.After
public void onTearDown() throws Exception {
		getMessages(); // drain queue
		jdbcTemplate.execute("delete from T_FOOS");
	}

	private void assertInitialState() {
		int count = jdbcTemplate.queryForObject("select count(*) from T_FOOS", Integer.class);
		assertEquals(0, count);
	}

	private List list = new ArrayList();

	private List recovered = new ArrayList();

	@org.junit.Test
public void testExternalRetryRecoveryInBatch() throws Exception {
		assertInitialState();

		retryTemplate.setRetryPolicy(new RecoveryCallbackRetryPolicy(new SimpleRetryPolicy(1)));

		repeatTemplate.setCompletionPolicy(new SimpleCompletionPolicy(2));

		// In a real container this could be an outer retry loop with an
		// *internal* retry policy.
		for (int i = 0; i < 4; i++) {
			try {
				new TransactionTemplate(transactionManager).execute(new TransactionCallback() {
					public Object doInTransaction(TransactionStatus status) {
						try {

							repeatTemplate.iterate(new RepeatCallback() {

								public ExitStatus doInIteration(RepeatContext context) throws Exception {

									final Object item = provider.read();
									
									if (item==null) {
										return ExitStatus.FINISHED;
									}
									
									RecoveryRetryCallback callback = new RecoveryRetryCallback(item, new RetryCallback() {
										public Object doWithRetry(RetryContext context) throws Throwable {
											// No need for transaction here: the whole batch will roll
											// back. When it comes back for recovery this code is not
											// executed...
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

									retryTemplate.execute(callback);
									
									return ExitStatus.CONTINUABLE;

								}

							});
							return null;

						} catch (Exception e) {
							throw new RuntimeException(e.getMessage(), e);
						}
					}
				});
			} catch (Exception e) {

				if (i == 0 || i == 2) {
					assertEquals("Rollback!", e.getMessage());
				} else {
					throw e;
				}

			} finally {
				System.err.println(i + ": " + recovered);
			}
		}

		List msgs = getMessages();

		System.err.println(msgs);

		assertEquals(2, recovered.size());

		// The database portion committed once...
		int count = jdbcTemplate.queryForObject("select count(*) from T_FOOS", Integer.class);
		assertEquals(0, count);

		// ... and so did the message session.
		// Both messages were failed and recovered after last retry attempt:
		assertEquals("[]", msgs.toString());
		assertEquals("[foo, bar]", recovered.toString());

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
