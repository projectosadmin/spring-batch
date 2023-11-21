package org.springframework.batch.core.repository.support;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.JobSupport;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.dao.AbstractDaoTest;
import org.springframework.batch.core.repository.dao.AbstractJobDaoTests;
import org.springframework.batch.core.step.StepSupport;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.ClassUtils;

import static org.junit.Assert.*;

/**
 * Repository tests using JDBC DAOs (rather than mocks).
 *
 * @author Robert Kasanicky
 */
@ContextConfiguration({"/org/springframework/batch/core/repository/dao/sql-dao-test.xml"})
public class SimpleJobRepositoryIntegrationTests extends AbstractDaoTest {

/*	protected String[] getConfigLocations() {
		return new String[] { ClassUtils.addResourcePathToPackagePath(AbstractJobDaoTests.class, "sql-dao-test.xml") };
	}*/

    @Autowired
    private SimpleJobRepository jobRepository;

    private JobSupport job = new JobSupport("SimpleJobRepositoryIntegrationTestsJob");

    private JobParameters jobParameters = new JobParameters();

    public void setJobRepository(SimpleJobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    /**
     * Create two job executions for same job+parameters tuple. Check both
     * executions belong to the same job instance and job.
     */
    @org.junit.Test
    public void testCreateAndFind() throws Exception {

        job.setRestartable(true);

        Map stringParams = new HashMap() {
            {
                put("stringKey", "stringValue");
            }
        };
        Map longParams = new HashMap() {
            {
                put("longKey", new Long(1));
            }
        };
        Map doubleParams = new HashMap() {
            {
                put("doubleKey", new Double(1.1));
            }
        };
        Map dateParams = new HashMap() {
            {
                put("dateKey", new Date(1));
            }
        };
        JobParameters jobParams = new JobParameters(stringParams, longParams, doubleParams, dateParams);

        JobExecution firstExecution = jobRepository.createJobExecution(job, jobParams);
        firstExecution.setStartTime(new Date());

        assertEquals(job.getName(), firstExecution.getJobInstance().getJobName());

        jobRepository.saveOrUpdate(firstExecution);
        firstExecution.setEndTime(new Date());
        jobRepository.saveOrUpdate(firstExecution);
        JobExecution secondExecution = jobRepository.createJobExecution(job, jobParams);

        assertEquals(firstExecution.getJobInstance(), secondExecution.getJobInstance());
        assertEquals(job.getName(), secondExecution.getJobInstance().getJobName());
    }

    /**
     * Create two job executions for same job+parameters tuple. Check both
     * executions belong to the same job instance and job.
     */
    @org.junit.Test
    public void testCreateAndFindWithNoStartDate() throws Exception {
        job.setRestartable(true);

        JobExecution firstExecution = jobRepository.createJobExecution(job, jobParameters);
        firstExecution.setStartTime(new Date(0));
        firstExecution.setEndTime(new Date(1));
        jobRepository.saveOrUpdate(firstExecution);
        JobExecution secondExecution = jobRepository.createJobExecution(job, jobParameters);

        assertEquals(firstExecution.getJobInstance(), secondExecution.getJobInstance());
        assertEquals(job.getName(), secondExecution.getJobInstance().getJobName());
    }

    /**
     * Non-restartable JobInstance can be run only once - attempt to run
     * existing non-restartable JobInstance causes error.
     */
    @org.junit.Test
    public void testRunNonRestartableJobInstanceTwice() throws Exception {
        job.setRestartable(false);

        JobExecution firstExecution = jobRepository.createJobExecution(job, jobParameters);
        jobRepository.saveOrUpdate(firstExecution);

        try {
            jobRepository.createJobExecution(job, jobParameters);
            fail();
        } catch (JobRestartException e) {
            // expected
        }
    }

    /**
     * Save multiple StepExecutions for the same step and check the returned
     * count and last execution are correct.
     */
    @org.junit.Test
    public void testGetStepExecutionCountAndLastStepExecution() throws Exception {
        job.setRestartable(true);
        StepSupport step = new StepSupport("restartedStep");

        // first execution
        JobExecution firstJobExec = jobRepository.createJobExecution(job, jobParameters);
        StepExecution firstStepExec = new StepExecution(step.getName(), firstJobExec);
        jobRepository.saveOrUpdate(firstJobExec);
        jobRepository.saveOrUpdate(firstStepExec);

        assertEquals(1, jobRepository.getStepExecutionCount(firstJobExec.getJobInstance(), step));
        assertEquals(firstStepExec, jobRepository.getLastStepExecution(firstJobExec.getJobInstance(), step));

        // first execution failed
        firstJobExec.setStartTime(new Date(4));
        firstStepExec.setStartTime(new Date(5));
        firstStepExec.setStatus(BatchStatus.FAILED);
        firstStepExec.setEndTime(new Date(6));
        jobRepository.saveOrUpdate(firstStepExec);
        firstJobExec.setStatus(BatchStatus.FAILED);
        firstJobExec.setEndTime(new Date(7));
        jobRepository.saveOrUpdate(firstJobExec);

        // second execution
        JobExecution secondJobExec = jobRepository.createJobExecution(job, jobParameters);
        StepExecution secondStepExec = new StepExecution(step.getName(), secondJobExec);
        jobRepository.saveOrUpdate(secondJobExec);
        jobRepository.saveOrUpdate(secondStepExec);

        assertEquals(2, jobRepository.getStepExecutionCount(secondJobExec.getJobInstance(), step));
        assertEquals(secondStepExec, jobRepository.getLastStepExecution(secondJobExec.getJobInstance(), step));
    }

    /**
     * Save execution context and retrieve it.
     */
    @org.junit.Test
    public void testSaveExecutionContext() throws Exception {
        ExecutionContext ctx = new ExecutionContext() {
            {
                putLong("crashedPosition", 7);
            }
        };
        JobExecution jobExec = jobRepository.createJobExecution(job, jobParameters);
        jobExec.setStartTime(new Date(0));
        jobExec.setExecutionContext(ctx);
        Step step = new StepSupport("step1");
        StepExecution stepExec = new StepExecution(step.getName(), jobExec);
        stepExec.setExecutionContext(ctx);

        jobRepository.saveOrUpdate(stepExec);
        jobRepository.saveOrUpdateExecutionContext(stepExec);

        StepExecution retrievedStepExec = jobRepository.getLastStepExecution(jobExec.getJobInstance(), step);
        assertEquals(stepExec, retrievedStepExec);
        assertEquals(ctx, retrievedStepExec.getExecutionContext());

//		JobExecution retrievedJobExec = jobRepository.getLastJobExecution(jobExec.getJobInstance());
//		assertEquals(jobExec, retrievedJobExec);
//		assertEquals(ctx, retrievedJobExec.getExecutionContext());
    }

    /**
     * If JobExecution is already running, exception will be thrown in attempt
     * to create new execution.
     */
    @org.junit.Test
    public void testOnlyOneJobExecutionAllowedRunning() throws Exception {
        job.setRestartable(true);
        jobRepository.createJobExecution(job, jobParameters);

        try {
            jobRepository.createJobExecution(job, jobParameters);
            fail();
        } catch (JobExecutionAlreadyRunningException e) {
            // expected
        }
    }

}