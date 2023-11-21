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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobInterruptedException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.UnexpectedJobExecutionException;
import org.springframework.batch.core.job.JobSupport;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.MapJobExecutionDao;
import org.springframework.batch.core.repository.dao.MapJobInstanceDao;
import org.springframework.batch.core.repository.dao.MapStepExecutionDao;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.batch.core.step.AbstractStep;
import org.springframework.batch.core.step.JobRepositorySupport;
import org.springframework.batch.core.step.StepInterruptionPolicy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.FlushFailedException;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.MarkFailedException;
import org.springframework.batch.item.ResetFailedException;
import org.springframework.batch.item.support.AbstractItemReader;
import org.springframework.batch.item.support.AbstractItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.ExitStatus;
import org.springframework.batch.repeat.policy.DefaultResultCompletionPolicy;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.batch.repeat.support.RepeatTemplate;
import org.springframework.batch.support.PropertiesConverter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.support.DefaultTransactionStatus;

public class ItemOrientedStepTests {

	List processed = new ArrayList();

	private List list = new ArrayList();

	ItemWriter itemWriter = new AbstractItemWriter() {
		public void write(Object data) throws Exception {
			processed.add((String) data);
		}
	};

	private ItemOrientedStep itemOrientedStep;

	private Job job;

	private JobInstance jobInstance;

	private ResourcelessTransactionManager transactionManager;

	private ItemReader getReader(String[] args) {
		return new ListItemReader(Arrays.asList(args));
	}

	private AbstractStep getStep(String[] strings) throws Exception {
		ItemOrientedStep step = new ItemOrientedStep("stepName");
		step.setItemHandler(new SimpleItemHandler(getReader(strings), itemWriter));
		step.setJobRepository(new JobRepositorySupport());
		step.setTransactionManager(transactionManager);
		return step;
	}

	    @org.junit.Before
public void setUp() throws Exception {
		MapJobInstanceDao.clear();
		MapStepExecutionDao.clear();
		MapJobExecutionDao.clear();

		transactionManager = new ResourcelessTransactionManager();

		RepeatTemplate template;

		itemOrientedStep = (ItemOrientedStep) getStep(new String[] { "foo", "bar", "spam" });
		template = new RepeatTemplate();
		template.setCompletionPolicy(new SimpleCompletionPolicy(1));
		itemOrientedStep.setStepOperations(template);
		// Only process one item:
		template = new RepeatTemplate();
		template.setCompletionPolicy(new SimpleCompletionPolicy(1));
		itemOrientedStep.setChunkOperations(template);

		job = new JobSupport("FOO");
		jobInstance = new JobInstance(new Long(0), new JobParameters(), job.getName());

		itemOrientedStep.setTransactionManager(transactionManager);

	}

	@org.junit.Test
public void testStepExecutor() throws Exception {

		JobExecution jobExecutionContext = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecutionContext);

		itemOrientedStep.execute(stepExecution);
		assertEquals(1, processed.size());
		assertEquals(1, stepExecution.getItemCount().intValue());
	}

	@org.junit.Test
public void testChunkExecutor() throws Exception {

		RepeatTemplate template = new RepeatTemplate();

		// Only process one item:
		template.setCompletionPolicy(new SimpleCompletionPolicy(1));
		itemOrientedStep.setChunkOperations(template);

		JobExecution jobExecution = new JobExecution(jobInstance);

		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecution);
		StepContribution contribution = stepExecution.createStepContribution();
		itemOrientedStep.processChunk(stepExecution, contribution);
		assertEquals(1, processed.size());
		assertEquals(0, stepExecution.getItemCount().intValue());
		assertEquals(1, contribution.getItemCount());

	}

	@org.junit.Test
public void testRepository() throws Exception {

		SimpleJobRepository repository = new SimpleJobRepository(new MapJobInstanceDao(), new MapJobExecutionDao(),
				new MapStepExecutionDao());
		itemOrientedStep.setJobRepository(repository);

		JobExecution jobExecution = repository.createJobExecution(job, jobInstance.getJobParameters());
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecution);

		itemOrientedStep.execute(stepExecution);
		assertEquals(1, processed.size());
	}

	@org.junit.Test
public void testIncrementRollbackCount() {

		ItemReader itemReader = new AbstractItemReader() {

			public Object read() throws Exception {
				int counter = 0;
				counter++;

				if (counter == 1) {
					throw new RuntimeException();
				}

				return ExitStatus.CONTINUABLE;
			}

		};

		itemOrientedStep.setItemHandler(new SimpleItemHandler(itemReader, itemWriter));
		JobExecution jobExecutionContext = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecutionContext);

		try {
			itemOrientedStep.execute(stepExecution);
		}
		catch (Exception ex) {
			assertEquals(stepExecution.getRollbackCount(), new Integer(1));
		}

	}

	@org.junit.Test
public void testExitCodeDefaultClassification() throws Exception {

		ItemReader itemReader = new AbstractItemReader() {

			public Object read() throws Exception {
				int counter = 0;
				counter++;

				if (counter == 1) {
					throw new RuntimeException();
				}

				return ExitStatus.CONTINUABLE;
			}

		};

		itemOrientedStep.setItemHandler(new SimpleItemHandler(itemReader, itemWriter));
		JobExecution jobExecutionContext = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecutionContext);

		try {
			itemOrientedStep.execute(stepExecution);
		}
		catch (Exception ex) {
			ExitStatus status = stepExecution.getExitStatus();
			assertFalse(status.isContinuable());
		}
	}

	@org.junit.Test
public void testExitCodeCustomClassification() throws Exception {

		ItemReader itemReader = new AbstractItemReader() {

			public Object read() throws Exception {
				int counter = 0;
				counter++;

				if (counter == 1) {
					throw new RuntimeException();
				}

				return ExitStatus.CONTINUABLE;
			}

		};

		itemOrientedStep.setItemHandler(new SimpleItemHandler(itemReader, itemWriter));
		itemOrientedStep.registerStepExecutionListener(new StepExecutionListenerSupport() {
			public ExitStatus onErrorInStep(StepExecution stepExecution, Throwable e) {
				return ExitStatus.FAILED.addExitDescription("FOO");
			}
		});
		JobExecution jobExecutionContext = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecutionContext);

		try {
			itemOrientedStep.execute(stepExecution);
		}
		catch (Exception ex) {
			ExitStatus status = stepExecution.getExitStatus();
			assertFalse(status.isContinuable());
			String description = status.getExitDescription();
			assertTrue("Description does not include 'FOO': " + description, description.indexOf("FOO") >= 0);
		}
	}

	/*
	 * make sure a job that has never been executed before, but does have
	 * saveExecutionAttributes = true, doesn't have restoreFrom called on it.
	 */
	@org.junit.Test
public void testNonRestartedJob() throws Exception {
		MockRestartableItemReader tasklet = new MockRestartableItemReader();
		itemOrientedStep.setItemHandler(new SimpleItemHandler(tasklet, itemWriter));
		itemOrientedStep.registerStream(tasklet);
		JobExecution jobExecutionContext = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecutionContext);

		itemOrientedStep.execute(stepExecution);

		assertFalse(tasklet.isRestoreFromCalled());
		assertTrue(tasklet.isGetExecutionAttributesCalled());
	}

	@org.junit.Test
public void testSuccessfulExecutionWithExecutionContext() throws Exception {
		final JobExecution jobExecution = new JobExecution(jobInstance);
		final StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecution);
		itemOrientedStep.setJobRepository(new JobRepositorySupport() {
			public void saveOrUpdateExecutionContext(StepExecution stepExecution) {
				list.add(stepExecution);
			}
		});
		itemOrientedStep.execute(stepExecution);

		// context saved before looping and updated once for every processing
		// loop (once in this case) and finally in the abstract step (regardless
		// of execution logic)
		assertEquals(3, list.size());
	}

	@org.junit.Test
public void testSuccessfulExecutionWithFailureOnSaveOfExecutionContext() throws Exception {
		final JobExecution jobExecution = new JobExecution(jobInstance);
		final StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecution);
		itemOrientedStep.setJobRepository(new JobRepositorySupport() {
			private int counter = 0;

			// initial save before item processing succeeds, later calls fail
			public void saveOrUpdateExecutionContext(StepExecution stepExecution) {
				if (counter > 0)
					throw new RuntimeException("foo");
				counter++;
			}
		});
		try {
			itemOrientedStep.execute(stepExecution);
			fail();
		}
		catch (RuntimeException e) {
			assertEquals("Fatal error detected during save of step execution context", e.getMessage());
			assertEquals("foo", e.getCause().getMessage());
		}
		assertEquals(BatchStatus.UNKNOWN, stepExecution.getStatus());
	}

	/*
	 * Test that a job that is being restarted, but has saveExecutionAttributes
	 * set to false, doesn't have restore or getExecutionAttributes called on
	 * it.
	 */
	@org.junit.Test
public void testNoSaveExecutionAttributesRestartableJob() {
		MockRestartableItemReader tasklet = new MockRestartableItemReader();
		itemOrientedStep.setItemHandler(new SimpleItemHandler(tasklet, itemWriter));
		JobExecution jobExecutionContext = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecutionContext);

		try {
			itemOrientedStep.execute(stepExecution);
		}
		catch (Throwable t) {
			fail();
		}

		assertFalse(tasklet.isRestoreFromCalled());
	}

	/*
	 * Even though the job is restarted, and saveExecutionAttributes is true,
	 * nothing will be restored because the Tasklet does not implement
	 * Restartable.
	 */
	@org.junit.Test
public void testRestartJobOnNonRestartableTasklet() throws Exception {
		itemOrientedStep.setItemHandler(new SimpleItemHandler(new AbstractItemReader() {
			public Object read() throws Exception {
				return "foo";
			}
		}, itemWriter));
		JobExecution jobExecution = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecution);

		itemOrientedStep.execute(stepExecution);
	}

	@org.junit.Test
public void testStreamManager() throws Exception {
		MockRestartableItemReader reader = new MockRestartableItemReader() {
			public Object read() throws Exception {
				return "foo";
			}

			public void update(ExecutionContext executionContext) {
				executionContext.putString("foo", "bar");
			}
		};
		itemOrientedStep.setItemHandler(new SimpleItemHandler(reader, itemWriter));
		itemOrientedStep.registerStream(reader);
		JobExecution jobExecution = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecution);

		assertEquals(false, stepExecution.getExecutionContext().containsKey("foo"));

		itemOrientedStep.execute(stepExecution);

		// At least once in that process the statistics service was asked for
		// statistics...
		assertEquals("bar", stepExecution.getExecutionContext().getString("foo"));
	}

	@org.junit.Test
public void testDirectlyInjectedItemStream() throws Exception {
		itemOrientedStep.setStreams(new ItemStream[] { new ItemStreamSupport() {
			public void update(ExecutionContext executionContext) {
				executionContext.putString("foo", "bar");
			}
		} });
		JobExecution jobExecution = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecution);

		assertEquals(false, stepExecution.getExecutionContext().containsKey("foo"));

		itemOrientedStep.execute(stepExecution);

		assertEquals("bar", stepExecution.getExecutionContext().getString("foo"));
	}

	@org.junit.Test
public void testDirectlyInjectedListener() throws Exception {
		itemOrientedStep.registerStepExecutionListener(new StepExecutionListenerSupport() {
			public void beforeStep(StepExecution stepExecution) {
				list.add("foo");
			}

			public ExitStatus afterStep(StepExecution stepExecution) {
				list.add("bar");
				return null;
			}
		});
		JobExecution jobExecution = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecution);
		itemOrientedStep.execute(stepExecution);
		assertEquals(2, list.size());
	}

	@org.junit.Test
public void testListenerCalledBeforeStreamOpened() throws Exception {
		MockRestartableItemReader reader = new MockRestartableItemReader() {
			public void beforeStep(StepExecution stepExecution) {
				list.add("foo");
			}

			public void open(ExecutionContext executionContext) throws ItemStreamException {
				assertEquals(1, list.size());
			}
		};
		itemOrientedStep.setStreams(new ItemStream[] { reader });
		itemOrientedStep.registerStepExecutionListener(reader);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), new JobExecution(jobInstance));
		itemOrientedStep.execute(stepExecution);
		assertEquals(1, list.size());
	}

	@org.junit.Test
public void testAfterStep() throws Exception {

		final ExitStatus customStatus = new ExitStatus(false, "custom code");

		itemOrientedStep.setStepExecutionListeners(new StepExecutionListener[] { new StepExecutionListenerSupport() {
			public ExitStatus afterStep(StepExecution stepExecution) {
				list.add("afterStepCalled");
				return customStatus;
			}
		} });

		RepeatTemplate stepTemplate = new RepeatTemplate();
		stepTemplate.setCompletionPolicy(new SimpleCompletionPolicy(5));
		itemOrientedStep.setStepOperations(stepTemplate);

		JobExecution jobExecution = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecution);
		itemOrientedStep.execute(stepExecution);
		assertEquals(1, list.size());
		ExitStatus returnedStatus = stepExecution.getExitStatus();
		assertEquals(customStatus.getExitCode(), returnedStatus.getExitCode());
		assertEquals(customStatus.getExitDescription(), returnedStatus.getExitDescription());
	}

	@org.junit.Test
public void testDirectlyInjectedListenerOnError() throws Exception {
		itemOrientedStep.registerStepExecutionListener(new StepExecutionListenerSupport() {
			public ExitStatus onErrorInStep(StepExecution stepExecution, Throwable e) {
				list.add(e);
				return null;
			}
		});
		itemOrientedStep.setItemHandler(new SimpleItemHandler(new MockRestartableItemReader() {
			public Object read() throws Exception {
				throw new RuntimeException("FOO");
			}
		}, itemWriter));
		JobExecution jobExecution = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecution);
		try {
			itemOrientedStep.execute(stepExecution);
			fail("Expected RuntimeException");
		}
		catch (RuntimeException e) {
			assertEquals("FOO", e.getMessage());
		}
		assertEquals(1, list.size());
	}

	@org.junit.Test
public void testDirectlyInjectedStreamWhichIsAlsoReader() throws Exception {
		MockRestartableItemReader reader = new MockRestartableItemReader() {
			public Object read() throws Exception {
				return "foo";
			}

			public void update(ExecutionContext executionContext) {
				executionContext.putString("foo", "bar");
			}
		};
		itemOrientedStep.setItemHandler(new SimpleItemHandler(reader, itemWriter));
		itemOrientedStep.setStreams(new ItemStream[] { reader });
		JobExecution jobExecution = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecution);

		assertEquals(false, stepExecution.getExecutionContext().containsKey("foo"));

		itemOrientedStep.execute(stepExecution);

		// At least once in that process the statistics service was asked for
		// statistics...
		assertEquals("bar", stepExecution.getExecutionContext().getString("foo"));
	}

	@org.junit.Test
public void testStatusForInterruptedException() {

		StepInterruptionPolicy interruptionPolicy = new StepInterruptionPolicy() {

			public void checkInterrupted(StepExecution stepExecution) throws JobInterruptedException {
				throw new JobInterruptedException("interrupted");
			}
		};

		itemOrientedStep.setInterruptionPolicy(interruptionPolicy);

		ItemReader itemReader = new AbstractItemReader() {

			public Object read() throws Exception {
				int counter = 0;
				counter++;

				if (counter == 1) {
					throw new RuntimeException();
				}

				return ExitStatus.CONTINUABLE;
			}

		};

		itemOrientedStep.setItemHandler(new SimpleItemHandler(itemReader, itemWriter));

		JobExecution jobExecutionContext = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecutionContext);

		stepExecution.setExecutionContext(new ExecutionContext(PropertiesConverter.stringToProperties("foo=bar")));

		try {
			itemOrientedStep.execute(stepExecution);
			fail("Expected JobInterruptedException");
		}
		catch (JobInterruptedException ex) {
			assertEquals(BatchStatus.STOPPED, stepExecution.getStatus());
			String msg = stepExecution.getExitStatus().getExitDescription();
			assertTrue("Message does not contain 'JobInterruptedException': " + msg, contains(msg,
					"JobInterruptedException"));
		}
	}

	@org.junit.Test
public void testStatusForNormalFailure() throws Exception {

		ItemReader itemReader = new AbstractItemReader() {
			public Object read() throws Exception {
				// Trigger a rollback
				throw new RuntimeException("Foo");
			}
		};
		itemOrientedStep.setItemHandler(new SimpleItemHandler(itemReader, itemWriter));

		JobExecution jobExecutionContext = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecutionContext);

		stepExecution.setExecutionContext(new ExecutionContext(PropertiesConverter.stringToProperties("foo=bar")));
		// step.setLastExecution(stepExecution);

		try {
			itemOrientedStep.execute(stepExecution);
			fail("Expected RuntimeException");
		}
		catch (RuntimeException ex) {
			assertEquals(BatchStatus.FAILED, stepExecution.getStatus());
			// The original rollback was caused by this one:
			assertEquals("Foo", ex.getMessage());
		}
	}

	@org.junit.Test
public void testStatusForErrorFailure() throws Exception {

		ItemReader itemReader = new AbstractItemReader() {
			public Object read() throws Exception {
				// Trigger a rollback
				throw new Error("Foo");
			}
		};
		itemOrientedStep.setItemHandler(new SimpleItemHandler(itemReader, itemWriter));

		JobExecution jobExecutionContext = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecutionContext);

		stepExecution.setExecutionContext(new ExecutionContext(PropertiesConverter.stringToProperties("foo=bar")));
		// step.setLastExecution(stepExecution);

		try {
			itemOrientedStep.execute(stepExecution);
			fail("Expected Error");
		}
		catch (Error ex) {
			assertEquals(BatchStatus.FAILED, stepExecution.getStatus());
			// The original rollback was caused by this one:
			assertEquals("Foo", ex.getMessage());
		}
	}

	@org.junit.Test
public void testStatusForResetFailedException() throws Exception {

		ItemReader itemReader = new AbstractItemReader() {
			public Object read() throws Exception {
				// Trigger a rollback
				throw new RuntimeException("Foo");
			}
		};
		itemOrientedStep.setItemHandler(new SimpleItemHandler(itemReader, itemWriter));
		itemOrientedStep.setTransactionManager(new ResourcelessTransactionManager() {
			protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
				// Simulate failure on rollback when stream resets
				throw new ResetFailedException("Bar");
			}
		});

		JobExecution jobExecutionContext = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecutionContext);

		stepExecution.setExecutionContext(new ExecutionContext(PropertiesConverter.stringToProperties("foo=bar")));
		// step.setLastExecution(stepExecution);

		try {
			itemOrientedStep.execute(stepExecution);
			fail("Expected UnexpectedJobExecutionException");
		}
		catch (RuntimeException ex) {
			assertEquals(BatchStatus.UNKNOWN, stepExecution.getStatus());
			String msg = stepExecution.getExitStatus().getExitDescription();
			assertTrue("Message does not contain ResetFailedException: " + msg, contains(msg, "ResetFailedException"));
			// The original rollback was caused by this one:
			assertEquals("Bar", ex.getCause().getMessage());
		}
	}

	@org.junit.Test
public void testStatusForCommitFailedException() throws Exception {

		itemOrientedStep.setTransactionManager(new ResourcelessTransactionManager() {
			protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
				// Simulate failure on commit
				throw new RuntimeException("Bar");
			}
		});

		JobExecution jobExecutionContext = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecutionContext);

		stepExecution.setExecutionContext(new ExecutionContext(PropertiesConverter.stringToProperties("foo=bar")));
		// step.setLastExecution(stepExecution);

		try {
			itemOrientedStep.execute(stepExecution);
			fail("Expected BatchCriticalException");
		}
		catch (RuntimeException ex) {
			assertEquals(BatchStatus.UNKNOWN, stepExecution.getStatus());
			String msg = stepExecution.getExitStatus().getExitDescription();
			assertTrue(msg.contains("Fatal error detected during commit"));
			msg = ex.getMessage();
			assertTrue(msg.contains("Fatal error detected during commit"));
			// The original rollback was caused by this one:
			assertEquals("Bar", ex.getCause().getMessage());
		}
	}

	@org.junit.Test
public void testStatusForFinalUpdateFailedException() throws Exception {

		itemOrientedStep.setJobRepository(new JobRepositorySupport());
		itemOrientedStep.setStreams(new ItemStream[] { new ItemStreamSupport() {
			public void close(ExecutionContext executionContext) throws ItemStreamException {
				throw new RuntimeException("Bar");
			}
		} });

		JobExecution jobExecutionContext = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecutionContext);

		try {
			itemOrientedStep.execute(stepExecution);
			fail("Expected RuntimeException");
		}
		catch (RuntimeException ex) {
			// The job actually completed, but the streams couldn't be closed.
			assertEquals(BatchStatus.COMPLETED, stepExecution.getStatus());
			String msg = stepExecution.getExitStatus().getExitDescription();
			assertEquals("", msg);
			msg = ex.getMessage();
			assertTrue("Message does not contain 'closing step': " + msg, contains(msg, "closing step"));
			// The original rollback was caused by this one:
			assertEquals("Bar", ex.getCause().getMessage());
		}
	}

	@org.junit.Test
public void testStatusForCloseFailedException() throws Exception {

		MockRestartableItemReader itemReader = new MockRestartableItemReader() {
			public void close(ExecutionContext executionContext) throws ItemStreamException {
				super.close(executionContext);
				// Simulate failure on rollback when stream resets
				throw new RuntimeException("Bar");
			}
		};
		itemOrientedStep.setItemHandler(new SimpleItemHandler(itemReader, itemWriter));
		itemOrientedStep.registerStream(itemReader);

		JobExecution jobExecutionContext = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecutionContext);

		stepExecution.setExecutionContext(new ExecutionContext(PropertiesConverter.stringToProperties("foo=bar")));
		// step.setLastExecution(stepExecution);

		try {
			itemOrientedStep.execute(stepExecution);
			fail("Expected InfrastructureException");
		}
		catch (UnexpectedJobExecutionException ex) {
			// The job actually completed, but the streams couldn't be closed.
			assertEquals(BatchStatus.COMPLETED, stepExecution.getStatus());
			String msg = stepExecution.getExitStatus().getExitDescription();
			assertEquals("", msg);
			msg = ex.getMessage();
			assertTrue("Message does not contain 'closing': " + msg, contains(msg, "closing"));
			// The original rollback was caused by this one:
			assertEquals("Bar", ex.getCause().getMessage());
		}
	}

	/**
	 * Execution context must not be left empty even if job failed before
	 * commiting first chunk - otherwise ItemStreams won't recognize it is
	 * restart scenario on next run.
	 */
	@org.junit.Test
public void testRestartAfterFailureInFirstChunk() throws Exception {
		MockRestartableItemReader reader = new MockRestartableItemReader() {
			public Object read() throws Exception {
				// fail on the very first item
				throw new RuntimeException("CRASH!");
			}
		};
		itemOrientedStep.setItemHandler(new SimpleItemHandler(reader, itemWriter));
		itemOrientedStep.registerStream(reader);

		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), new JobExecution(jobInstance));

		try {
			itemOrientedStep.execute(stepExecution);
			fail("Expected InfrastructureException");
		}
		catch (RuntimeException expected) {
			assertEquals(BatchStatus.FAILED, stepExecution.getStatus());
			assertEquals("CRASH!", expected.getMessage());
			assertFalse(stepExecution.getExecutionContext().isEmpty());
			assertTrue(stepExecution.getExecutionContext().getString("spam").equals("bucket"));
		}
	}

	@org.junit.Test
public void testStepToCompletion() throws Exception {

		RepeatTemplate template = new RepeatTemplate();

		// process all items:
		template.setCompletionPolicy(new DefaultResultCompletionPolicy());
		itemOrientedStep.setStepOperations(template);

		JobExecution jobExecutionContext = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecutionContext);

		itemOrientedStep.execute(stepExecution);
		assertEquals(3, processed.size());
		assertEquals(3, stepExecution.getItemCount().intValue());
	}

	/**
	 * Exception in {@link StepExecutionListener#afterStep(StepExecution)}
	 * causes step to fail.
	 * @throws JobInterruptedException JobInterruptedException
	 */
	@org.junit.Test
public void testStepFailureInAfterStepCallback() throws JobInterruptedException {
		StepExecutionListener listener = new StepExecutionListenerSupport() {
			public ExitStatus afterStep(StepExecution stepExecution) {
				throw new RuntimeException("exception thrown in afterStep to signal failure");
			}
		};
		itemOrientedStep.setStepExecutionListeners(new StepExecutionListener[] { listener });
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), new JobExecution(jobInstance));
		try {
			itemOrientedStep.execute(stepExecution);
			fail();
		}
		catch (RuntimeException expected) {
			assertEquals("exception thrown in afterStep to signal failure", expected.getMessage());
		}

		assertEquals(BatchStatus.FAILED, stepExecution.getStatus());

	}

	@org.junit.Test
public void testStepExecutionUpdates() throws Exception {

		JobExecution jobExecution = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecution);

		itemOrientedStep.setStepOperations(new RepeatTemplate());

		JobRepositoryStub jobRepository = new JobRepositoryStub();
		itemOrientedStep.setJobRepository(jobRepository);

		itemOrientedStep.execute(stepExecution);

		assertEquals(3, processed.size());
		assertEquals(3, stepExecution.getItemCount().intValue());
		assertTrue(3 <= jobRepository.updateCount);
	}
	
	
	/**
	 * Failure to update StepExecution after chunk commit is fatal.
	 */
	@org.junit.Test
public void testStepExecutionUpdateFailure() throws Exception {

		JobExecution jobExecution = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecution);

		JobRepository repository = new JobRepositoryFailedUpdateStub();

		itemOrientedStep.setJobRepository(repository);
		itemOrientedStep.afterPropertiesSet();

		try {
			itemOrientedStep.execute(stepExecution);
			fail();
		}
		catch (Exception e) {
			assertEquals(BatchStatus.UNKNOWN, stepExecution.getStatus());
			assertEquals("Fatal error detected during update of step execution", e.getMessage());
			assertEquals("stub exception", e.getCause().getMessage());
		}
		
	}
	
	@org.junit.Test
public void testNoRollbackOnFlushException() throws Exception{
		
		final RuntimeException exception = new RuntimeException();
		ItemWriter itemWriter = new AbstractItemWriter() {
			public void flush() throws FlushFailedException {
				throw exception;
			}

			public void write(Object item) throws Exception {	}
		};
		itemOrientedStep.setItemHandler(new SimpleItemHandler(new MockRestartableItemReader(), itemWriter));
		itemOrientedStep.setTransactionAttribute(new NeverRollbackTransactionAttribute());
		
		JobExecution jobExecutionContext = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecutionContext);

		stepExecution.setExecutionContext(new ExecutionContext(PropertiesConverter.stringToProperties("foo=bar")));
		// step.setLastExecution(stepExecution);

		try {
			itemOrientedStep.execute(stepExecution);
			fail("Expected RuntimeException");
		}
		catch (RuntimeException ex) {
			assertEquals(BatchStatus.FAILED, stepExecution.getStatus());
			// The original rollback was caused by this one:
			assertSame(exception, ex);
		}
	}
	
	@org.junit.Test
public void testNoRollbackOnUpdateException() throws Exception{
		
		final RuntimeException exception = new RuntimeException();

		itemOrientedStep.setStreams(new ItemStream[]{new ItemStreamSupport(){
			int updateCount = 0;
			
			public void update(ExecutionContext executionContext) {
				if(updateCount > 0){
					throw exception;
				}
				
				updateCount++;
			}
		}});
		
		itemOrientedStep.setItemHandler(new SimpleItemHandler(new MockRestartableItemReader(), itemWriter));
		itemOrientedStep.setTransactionAttribute(new NeverRollbackTransactionAttribute());
		
		JobExecution jobExecutionContext = new JobExecution(jobInstance);
		StepExecution stepExecution = new StepExecution(itemOrientedStep.getName(), jobExecutionContext);

		stepExecution.setExecutionContext(new ExecutionContext(PropertiesConverter.stringToProperties("foo=bar")));
		// step.setLastExecution(stepExecution);

		try {
			itemOrientedStep.execute(stepExecution);
			fail("Expected RuntimeException");
		}
		catch (RuntimeException ex) {
			assertEquals(BatchStatus.FAILED, stepExecution.getStatus());
			// The original rollback was caused by this one:
			assertSame(exception, ex);
		}
	}
	
	private static class JobRepositoryFailedUpdateStub extends JobRepositorySupport {
		
		private int timesCalled = 0;
		
		public void saveOrUpdate(StepExecution stepExecution) {
			timesCalled++;
			if (timesCalled == 2) {
				throw new DataAccessResourceFailureException("stub exception");
			}
		}
	}

	private static class JobRepositoryStub extends JobRepositorySupport {

		private int updateCount = -1;

		public void saveOrUpdate(StepExecution stepExecution) {
			updateCount++;
			if (updateCount <= 3) {
				assertEquals(Integer.valueOf(updateCount), stepExecution.getItemCount());
			}
		}

	}

	private boolean contains(String str, String searchStr) {
		return str.indexOf(searchStr) != -1;
	}

	private class MockRestartableItemReader extends ItemStreamSupport implements ItemReader, StepExecutionListener {

		private boolean getExecutionAttributesCalled = false;

		private boolean restoreFromCalled = false;

		private boolean restoreFromCalledWithSomeContext = false;

		public Object read() throws Exception {
			return "item";
		}

		public boolean isRestoreFromCalledWithSomeContext() {
			return restoreFromCalledWithSomeContext;
		}

		public void update(ExecutionContext executionContext) {
			getExecutionAttributesCalled = true;
			executionContext.putString("spam", "bucket");
		}

		public boolean isGetExecutionAttributesCalled() {
			return getExecutionAttributesCalled;
		}

		public boolean isRestoreFromCalled() {
			return restoreFromCalled;
		}

		public void mark() throws MarkFailedException {
		}

		public void reset() throws ResetFailedException {
		}

		public ExitStatus afterStep(StepExecution stepExecution) {
			return null;
		}

		public void beforeStep(StepExecution stepExecution) {
		}

		public ExitStatus onErrorInStep(StepExecution stepExecution, Throwable e) {
			return null;
		}

	}
	
	private static class NeverRollbackTransactionAttribute implements TransactionAttribute{

		@Override
		public String getQualifier() {
			return null;
		}

		//@Override
		public Collection<String> getLabels() {
			return null;
		}

		public boolean rollbackOn(Throwable ex) {
			// TODO Auto-generated method stub
			return false;
		}

		public int getIsolationLevel() {
			// TODO Auto-generated method stub
			return 0;
		}

		public String getName() {
			// TODO Auto-generated method stub
			return null;
		}

		public int getPropagationBehavior() {
			// TODO Auto-generated method stub
			return 0;
		}

		public int getTimeout() {
			// TODO Auto-generated method stub
			return 0;
		}

		public boolean isReadOnly() {
			// TODO Auto-generated method stub
			return false;
		}
		
	}

}
