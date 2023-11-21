package org.springframework.batch.core.step;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobInterruptedException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.ExitStatus;
import org.springframework.util.Assert;

/**
 * Tests for {@link AbstractStep}.
 */
public class AbstractStepTests {

    AbstractStep tested = new EventTrackingStep();

    StepExecutionListener listener1 = new EventTrackingListener("listener1");

    StepExecutionListener listener2 = new EventTrackingListener("listener2");

    JobRepositoryStub repository = new JobRepositoryStub();

    /**
     * Sequence of events encountered during step execution.
     */
    final List events = new ArrayList();

    final StepExecution execution = new StepExecution(tested.getName(), new JobExecution(new JobInstance(new Long(1),
            new JobParameters(), "jobName")));

    /**
     * Fills the events list when abstract methods are called.
     */
    private class EventTrackingStep extends AbstractStep {

        public EventTrackingStep() {
            setBeanName("eventTrackingStep");
        }

        protected void open(ExecutionContext ctx) throws Exception {
            events.add("open");
        }

        protected ExitStatus doExecute(StepExecution stepExecution) throws Exception {
            assertSame(execution, stepExecution);
            events.add("doExecute");
            return ExitStatus.FINISHED;
        }

        protected void close(ExecutionContext ctx) throws Exception {
            events.add("close");
        }
    }

    /**
     * Fills the events list when listener methods are called, prefixed with the
     * name of the listener.
     */
    private class EventTrackingListener implements StepExecutionListener {

        private String name;

        public EventTrackingListener(String name) {
            this.name = name;
        }

        private String getEvent(String event) {
            return name + "#" + event;
        }

        public ExitStatus afterStep(StepExecution stepExecution) {
            assertSame(execution, stepExecution);
            events.add(getEvent("afterStep"));
            stepExecution.getExecutionContext().putString("afterStep", "afterStep");
            return stepExecution.getExitStatus();
        }

        public ExitStatus onErrorInStep(StepExecution stepExecution, Throwable e) {
            assertSame(execution, stepExecution);
            events.add(getEvent("onErrorInStep"));
            stepExecution.getExecutionContext().putString("onErrorInStep", "onErrorInStep");
            return stepExecution.getExitStatus();
        }

        public void beforeStep(StepExecution stepExecution) {
            assertSame(execution, stepExecution);
            events.add(getEvent("beforeStep"));
            stepExecution.getExecutionContext().putString("beforeStep", "beforeStep");
        }

    }

    /**
     * Remembers the last saved values of execution context.
     */
    private static class JobRepositoryStub extends JobRepositorySupport {

        static long counter = 0;

        ExecutionContext saved = new ExecutionContext();

        public void saveOrUpdateExecutionContext(StepExecution stepExecution) {
            Assert.state(stepExecution.getId() != null, "StepExecution must be already saved");
            saved = stepExecution.getExecutionContext();
        }

        public void saveOrUpdate(StepExecution stepExecution) {
            if (stepExecution.getId() == null) {
                stepExecution.setId(new Long(counter));
                counter++;
            }
        }


    }

    @org.junit.Before
    public void setUp() throws Exception {
        tested.setJobRepository(repository);
    }

    /**
     * Typical step execution scenario.
     */
    @org.junit.Test
    public void testExecute() throws Exception {
        tested.setStepExecutionListeners(new StepExecutionListener[]{listener1, listener2});
        tested.execute(execution);

        int i = 0;
        assertEquals("listener1#beforeStep", events.get(i++));
        assertEquals("listener2#beforeStep", events.get(i++));
        assertEquals("open", events.get(i++));
        assertEquals("doExecute", events.get(i++));
        assertEquals("listener2#afterStep", events.get(i++));
        assertEquals("listener1#afterStep", events.get(i++));
        assertEquals("close", events.get(i++));
        assertEquals(7, events.size());

        assertEquals(ExitStatus.FINISHED, execution.getExitStatus());

        assertTrue("Execution context modifications made by listener should be persisted", repository.saved
                .containsKey("beforeStep"));
        assertTrue("Execution context modifications made by listener should be persisted", repository.saved
                .containsKey("afterStep"));
    }

    /**
     * Exception during business processing.
     */
    @org.junit.Test
    public void testFailure() throws Exception {
        tested = new EventTrackingStep() {
            protected ExitStatus doExecute(StepExecution stepExecution) throws Exception {
                super.doExecute(stepExecution);
                throw new RuntimeException("crash!");
            }
        };
        tested.setJobRepository(repository);
        tested.setStepExecutionListeners(new StepExecutionListener[]{listener1, listener2});

        try {
            tested.execute(execution);
            fail();
        } catch (RuntimeException expected) {
            assertEquals("crash!", expected.getMessage());
        }

        int i = 0;
        assertEquals("listener1#beforeStep", events.get(i++));
        assertEquals("listener2#beforeStep", events.get(i++));
        assertEquals("open", events.get(i++));
        assertEquals("doExecute", events.get(i++));
        assertEquals("listener2#onErrorInStep", events.get(i++));
        assertEquals("listener1#onErrorInStep", events.get(i++));
        assertEquals("close", events.get(i++));
        assertEquals(7, events.size());

        assertEquals(ExitStatus.FAILED.getExitCode(), execution.getExitStatus().getExitCode());
        String exitDescription = execution.getExitStatus().getExitDescription();
        assertTrue("Wrong message: " + exitDescription, exitDescription.contains("crash"));

        assertTrue("Execution context modifications made by listener should be persisted", repository.saved
                .containsKey("onErrorInStep"));
    }

    /**
     * Exception during business processing.
     */
    @org.junit.Test
    public void testStoppedStep() throws Exception {
        tested = new EventTrackingStep() {
            protected ExitStatus doExecute(StepExecution stepExecution) throws Exception {
                stepExecution.setTerminateOnly();
                return super.doExecute(stepExecution);
            }
        };
        tested.setJobRepository(repository);
        tested.setStepExecutionListeners(new StepExecutionListener[]{listener1, listener2});

        try {
            tested.execute(execution);
            fail();
        } catch (JobInterruptedException expected) {
            assertEquals("JobExecution interrupted.", expected.getMessage());
        }

        int i = 0;
        assertEquals("listener1#beforeStep", events.get(i++));
        assertEquals("listener2#beforeStep", events.get(i++));
        assertEquals("open", events.get(i++));
        assertEquals("doExecute", events.get(i++));
        assertEquals("listener2#onErrorInStep", events.get(i++));
        assertEquals("listener1#onErrorInStep", events.get(i++));
        assertEquals("close", events.get(i++));
        assertEquals(7, events.size());

        assertEquals("JOB_INTERRUPTED", execution.getExitStatus().getExitCode());

        assertTrue("Execution context modifications made by listener should be persisted", repository.saved
                .containsKey("onErrorInStep"));
    }

    /**
     * Exception during business processing.
     */
    @org.junit.Test
    public void testFailureInSavingExecutionContext() throws Exception {
        tested = new EventTrackingStep() {
            protected ExitStatus doExecute(StepExecution stepExecution) throws Exception {
                super.doExecute(stepExecution);
                return ExitStatus.FINISHED;
            }
        };
        repository = new JobRepositoryStub() {
            public void saveOrUpdateExecutionContext(StepExecution stepExecution) {
                throw new RuntimeException("Bad context!");
            }
        };
        tested.setJobRepository(repository);

        try {
            tested.execute(execution);
            fail();
        } catch (RuntimeException expected) {
            assertEquals("Bad context!", expected.getCause().getMessage());
        }

        int i = 0;
        assertEquals("open", events.get(i++));
        assertEquals("doExecute", events.get(i++));
        assertEquals("close", events.get(i++));
        assertEquals(3, events.size());

        assertEquals(ExitStatus.UNKNOWN.getExitCode(), execution.getExitStatus().getExitCode());
    }

    /**
     * JobRepository is a required property.
     */
    @org.junit.Test
    public void testAfterPropertiesSet() throws Exception {
        tested.setJobRepository(null);
        try {
            tested.afterPropertiesSet();
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

}
