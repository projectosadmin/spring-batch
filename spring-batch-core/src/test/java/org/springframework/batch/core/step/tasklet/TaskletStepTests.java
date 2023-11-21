package org.springframework.batch.core.step.tasklet;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobInterruptedException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.UnexpectedJobExecutionException;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.core.step.JobRepositorySupport;
import org.springframework.batch.repeat.ExitStatus;

public class TaskletStepTests {

	private StepExecution stepExecution;

	private List list = new ArrayList();

	    @org.junit.Before
public void setUp() throws Exception {
		stepExecution = new StepExecution("stepName", new JobExecution(new JobInstance(new Long(0L),
				new JobParameters(), "testJob"), new Long(12)));
	}

	@org.junit.Test
public void testTaskletMandatory() throws Exception {
		TaskletStep step = new TaskletStep();
		step.setJobRepository(new JobRepositorySupport());
		try {
			step.afterPropertiesSet();
		}
		catch (IllegalArgumentException e) {
			String message = e.getMessage();
			assertTrue("Message should contain 'tasklet': " + message, contains(message.toLowerCase(), "tasklet"));
		}
	}

	@org.junit.Test
public void testRepositoryMandatory() throws Exception {
		TaskletStep step = new TaskletStep();
		try {
			step.afterPropertiesSet();
			fail();
		}
		catch (IllegalArgumentException e) {
			String message = e.getMessage();
			assertTrue("Message should contain 'mandatory': " + message, contains(message.toLowerCase(), "mandatory"));
		}
	}

	@org.junit.Test
public void testSuccessfulExecution() throws Exception {
		TaskletStep step = new TaskletStep(new StubTasklet(false, false), new JobRepositorySupport());
		step.execute(stepExecution);
		assertNotNull(stepExecution.getStartTime());
		assertEquals(ExitStatus.FINISHED, stepExecution.getExitStatus());
		assertNotNull(stepExecution.getEndTime());
	}

	@org.junit.Test
public void testSuccessfulExecutionWithStepContext() throws Exception {
		TaskletStep step = new TaskletStep(new StubTasklet(false, false, true), new JobRepositorySupport());
		step.afterPropertiesSet();
		step.execute(stepExecution);
		assertNotNull(stepExecution.getStartTime());
		assertEquals(ExitStatus.FINISHED, stepExecution.getExitStatus());
		assertNotNull(stepExecution.getEndTime());
	}

	@org.junit.Test
public void testSuccessfulExecutionWithExecutionContext() throws Exception {
		TaskletStep step = new TaskletStep(new StubTasklet(false, false), new JobRepositorySupport() {
			public void saveOrUpdateExecutionContext(StepExecution stepExecution) {
				list.add(stepExecution);
			}
		});
		step.execute(stepExecution);
		assertEquals(1, list.size());
	}

	@org.junit.Test
public void testSuccessfulExecutionWithFailureOnSaveOfExecutionContext() throws Exception {
		TaskletStep step = new TaskletStep(new StubTasklet(false, false, true), new JobRepositorySupport() {
			public void saveOrUpdateExecutionContext(StepExecution stepExecution) {
				throw new RuntimeException("foo");
			}
		});
		step.afterPropertiesSet();
		try {
			step.execute(stepExecution);
			fail("Expected BatchCriticalException");
		}
		catch (UnexpectedJobExecutionException e) {
			assertEquals("foo", e.getCause().getMessage());
		}
		assertEquals(BatchStatus.UNKNOWN, stepExecution.getStatus());
	}

	@org.junit.Test
public void testFailureExecution() throws Exception {
		TaskletStep step = new TaskletStep(new StubTasklet(true, false), new JobRepositorySupport());
		step.execute(stepExecution);
		assertNotNull(stepExecution.getStartTime());
		assertEquals(ExitStatus.FAILED, stepExecution.getExitStatus());
		assertNotNull(stepExecution.getEndTime());
	}

	@org.junit.Test
public void testSuccessfulExecutionWithListener() throws Exception {
		TaskletStep step = new TaskletStep(new StubTasklet(false, false), new JobRepositorySupport());
		step.setStepExecutionListeners(new StepExecutionListener[] { new StepExecutionListenerSupport() {
			public void beforeStep(StepExecution context) {
				list.add("open");
			}

			public ExitStatus afterStep(StepExecution stepExecution) {
				list.add("close");
				return ExitStatus.CONTINUABLE;
			}
		} });
		step.execute(stepExecution);
		assertEquals(2, list.size());
	}

	@org.junit.Test
public void testExceptionExecution() throws JobInterruptedException, UnexpectedJobExecutionException {
		TaskletStep step = new TaskletStep(new StubTasklet(false, true), new JobRepositorySupport());
		try {
			step.execute(stepExecution);
			fail();
		}
		catch (RuntimeException e) {
			assertNotNull(stepExecution.getStartTime());
			assertEquals(ExitStatus.FAILED.getExitCode(), stepExecution.getExitStatus().getExitCode());
			assertNotNull(stepExecution.getEndTime());
		}
	}

	@org.junit.Test
public void testExceptionError() throws JobInterruptedException, UnexpectedJobExecutionException {
		TaskletStep step = new TaskletStep(new StubTasklet(new Error("Foo!")), new JobRepositorySupport());
		try {
			step.execute(stepExecution);
			fail();
		}
		catch (Error e) {
			assertNotNull(stepExecution.getStartTime());
			assertEquals(ExitStatus.FAILED.getExitCode(), stepExecution.getExitStatus().getExitCode());
			assertNotNull(stepExecution.getEndTime());
		}
	}

	/**
	 * When job is interrupted the {@link JobInterruptedException} should be
	 * propagated up.
	 */
	@org.junit.Test
public void testJobInterrupted() throws Exception {
		TaskletStep step = new TaskletStep(new Tasklet() {
			public ExitStatus execute() throws Exception {
				throw new JobInterruptedException("Job interrupted while executing tasklet");
			}
		}, new JobRepositorySupport());

		try {
			step.execute(stepExecution);
			fail();
		}
		catch (JobInterruptedException expected) {
			assertEquals("Job interrupted while executing tasklet", expected.getMessage());
		}
	}
	
	/**
	 * Exception in {@link StepExecutionListener#afterStep(StepExecution)}
	 * causes step to fail.
	 * @throws JobInterruptedException JobInterruptedException
	 */
	@org.junit.Test
public void testStepFailureInAfterStepCallback() throws JobInterruptedException {
		TaskletStep step = new TaskletStep(new Tasklet() {
			public ExitStatus execute() throws Exception {
				return ExitStatus.FINISHED;
			}
		}, new JobRepositorySupport());
		
		StepExecutionListener listener = new StepExecutionListenerSupport() {
			public ExitStatus afterStep(StepExecution stepExecution) {
				throw new RuntimeException("exception thrown in afterStep to signal failure");
			}
		};
		step.setStepExecutionListeners(new StepExecutionListener[] { listener });
		try {
			step.execute(stepExecution);
			fail();
		}
		catch (RuntimeException expected) {
			assertEquals("exception thrown in afterStep to signal failure", expected.getMessage());
		}
		
		assertEquals(BatchStatus.FAILED, stepExecution.getStatus());
		
	}

	private static class StubTasklet extends StepExecutionListenerSupport implements Tasklet {

		private final boolean exitFailure;

		private final boolean throwException;

		private final boolean assertStepContext;

		private StepExecution stepExecution;

		private Throwable exception = null;

		public StubTasklet(boolean exitFailure, boolean throwException) {
			this(exitFailure, throwException, false);
		}

		public StubTasklet(boolean exitFailure, boolean throwException, boolean assertStepContext) {
			this.exitFailure = exitFailure;
			this.throwException = throwException;
			this.assertStepContext = assertStepContext;
		}

		/**
		 * @param error error
		 */
		public StubTasklet(Throwable error) {
			this(false, false, false);
			this.exception = error;
		}

		public ExitStatus execute() throws Exception {
			if (throwException) {
				throw new Exception();
			}
			
			if (exception!=null) {
				if (exception instanceof Exception) throw (Exception) exception;
				if (exception instanceof Error) throw (Error) exception;
			}

			if (exitFailure) {
				return ExitStatus.FAILED;
			}

			if (assertStepContext) {
				assertNotNull(this.stepExecution);
			}

			return ExitStatus.FINISHED;
		}

		public void beforeStep(StepExecution stepExecution) {
			this.stepExecution = stepExecution;
		}

	}

	private boolean contains(String str, String searchStr) {
		return str.indexOf(searchStr) != -1;
	}
}
