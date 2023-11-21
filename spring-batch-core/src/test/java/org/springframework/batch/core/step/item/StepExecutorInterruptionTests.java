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

import static org.junit.Assert.*;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInterruptedException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.JobSupport;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.MapJobExecutionDao;
import org.springframework.batch.core.repository.dao.MapJobInstanceDao;
import org.springframework.batch.core.repository.dao.MapStepExecutionDao;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.batch.core.step.StepExecutionSynchronizer;
import org.springframework.batch.core.step.item.ItemOrientedStep;
import org.springframework.batch.core.step.item.SimpleItemHandler;
import org.springframework.batch.item.support.AbstractItemReader;
import org.springframework.batch.item.support.AbstractItemWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.batch.repeat.support.RepeatTemplate;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;

public class StepExecutorInterruptionTests {

	private ItemOrientedStep step;

	private JobExecution jobExecution;

	private AbstractItemWriter itemWriter;

	private StepExecution stepExecution;

	    @org.junit.Before
public void setUp() throws Exception {
		MapJobInstanceDao.clear();
		MapJobExecutionDao.clear();
		MapStepExecutionDao.clear();

		JobRepository jobRepository = new SimpleJobRepository(new MapJobInstanceDao(), new MapJobExecutionDao(),
				new MapStepExecutionDao());

		JobSupport jobConfiguration = new JobSupport();
		step = new ItemOrientedStep("interruptedStep");
		jobConfiguration.addStep(step);
		jobConfiguration.setBeanName("testJob");
		jobExecution = jobRepository.createJobExecution(jobConfiguration, new JobParameters());
		step.setJobRepository(jobRepository);
		step.setTransactionManager(new ResourcelessTransactionManager());
		itemWriter = new AbstractItemWriter() {
			public void write(Object item) throws Exception {
			}
		};
		step.setItemHandler(new SimpleItemHandler(new AbstractItemReader() {
			public Object read() throws Exception {
				return null;
			}
		}, itemWriter));
		stepExecution = new StepExecution(step.getName(), jobExecution);
	}

	@org.junit.Test
public void testInterruptChunk() throws Exception {

		Thread processingThread = createThread(stepExecution);

		processingThread.start();
		Thread.sleep(100);
		processingThread.interrupt();

		int count = 0;
		while (processingThread.isAlive() && count < 1000) {
			Thread.sleep(20);
			count++;
		}

		assertTrue("Timed out waiting for step to be interrupted.", count < 1000);
		assertFalse(processingThread.isAlive());
		assertEquals(BatchStatus.STOPPED, stepExecution.getStatus());

	}

	@org.junit.Test
public void testInterruptStep() throws Exception {
		RepeatTemplate template = new RepeatTemplate();
		// N.B, If we don't set the completion policy it might run forever
		template.setCompletionPolicy(new SimpleCompletionPolicy(2));
		step.setChunkOperations(template);
		testInterruptChunk();
	}

	@org.junit.Test
public void testInterruptOnInterruptedException() throws Exception {

		Thread processingThread = createThread(stepExecution);

		step.setItemHandler(new SimpleItemHandler(new AbstractItemReader() {
			public Object read() throws Exception {
				return null;
			}
		}, itemWriter));

		// This simulates the unlikely sounding, but in practice all too common
		// in Bamboo situation where the thread is interrupted before the lock
		// is taken.
		step.setSynchronizer(new StepExecutionSynchronizer() {
			public void lock(StepExecution stepExecution) throws InterruptedException {
				Thread.currentThread().interrupt();
				throw new InterruptedException();
			}

			public void release(StepExecution stepExecution) {
			}
		});

		processingThread.start();
		Thread.sleep(100);

		int count = 0;
		while (processingThread.isAlive() && count < 1000) {
			Thread.sleep(20);
			count++;
		}

		assertTrue("Timed out waiting for step to be interrupted.", count < 1000);
		assertFalse(processingThread.isAlive());
		assertEquals(BatchStatus.STOPPED, stepExecution.getStatus());

	}

	@org.junit.Test
public void testLockNotReleasedIfChunkFails() throws Exception {

		step.setItemHandler(new SimpleItemHandler(new AbstractItemReader() {
			public Object read() throws Exception {
				throw new RuntimeException("Planned!");
			}
		}, itemWriter));

		step.setSynchronizer(new StepExecutionSynchronizer() {
			private boolean locked = false;
			public void lock(StepExecution stepExecution) throws InterruptedException {
				locked = true;
			}
			public void release(StepExecution stepExecution) {
				assertTrue("Lock released before it is acquired", locked);
			}
		});
		
		try {
			step.execute(stepExecution);
			fail("Expected planned RuntimeException");
		} catch (RuntimeException e) {
			assertEquals("Planned!", e.getMessage());
		}

		assertEquals(BatchStatus.FAILED, stepExecution.getStatus());

	}

	/**
	 * @return
	 */
	private Thread createThread(final StepExecution stepExecution) {
		step.setItemHandler(new SimpleItemHandler(new AbstractItemReader() {
			public Object read() throws Exception {
				// do something non-trivial (and not Thread.sleep())
				double foo = 1;
				for (int i = 2; i < 250; i++) {
					foo = foo * i;
				}

				if (foo != 1) {
					return new Double(foo);
				}
				else {
					return null;
				}
			}
		}, itemWriter));

		Thread processingThread = new Thread() {
			public void run() {
				try {
					step.execute(stepExecution);
				}
				catch (JobInterruptedException e) {
					// do nothing...
				}
			}
		};
		return processingThread;
	}

}
