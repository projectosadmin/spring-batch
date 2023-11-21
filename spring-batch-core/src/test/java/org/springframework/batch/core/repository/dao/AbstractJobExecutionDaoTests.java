package org.springframework.batch.core.repository.dao;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.item.ExecutionContext;

import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;


public abstract class AbstractJobExecutionDaoTests extends AbstractDaoTest {

    JobExecutionDao dao;

    abstract JobExecutionDao getJobExecutionDao();

    JobInstance jobInstance = new JobInstance(new Long(1), new JobParameters(), "execTestJob");

    JobExecution execution = new JobExecution(jobInstance);

    @Before
    public void init() {
        dao = getJobExecutionDao();
    }

    /**
     * Save and find a job execution.
     */
@org.junit.Test
public void testSaveAndFind() {

        dao.saveJobExecution(execution);

        List executions = dao.findJobExecutions(jobInstance);
        assertTrue(executions.size() == 1);
        assertEquals(execution, executions.get(0));
    }

    /**
     * Saving sets id to the entity.
     */
@org.junit.Test
public void testSaveAddsIdAndVersion() {

        assertNull(execution.getId());
        assertNull(execution.getVersion());
        dao.saveJobExecution(execution);
        assertNotNull(execution.getId());
        assertNotNull(execution.getVersion());
    }

    /**
     * Execution count increases by one with every save for the same job
     * instance.
     */
@org.junit.Test
public void testGetExecutionCount() {

        JobExecution exec1 = new JobExecution(jobInstance);
        JobExecution exec2 = new JobExecution(jobInstance);

        dao.saveJobExecution(exec1);
        assertEquals(1, dao.getJobExecutionCount(jobInstance));

        dao.saveJobExecution(exec2);
        assertEquals(2, dao.getJobExecutionCount(jobInstance));
    }

    /**
     * Update and retrieve job execution - check attributes have changed as
     * expected.
     */
@org.junit.Test
public void testUpdateExecution() {
        execution.setStatus(BatchStatus.STARTED);
        dao.saveJobExecution(execution);

        execution.setStatus(BatchStatus.COMPLETED);
        dao.updateJobExecution(execution);

        JobExecution updated = (JobExecution) dao.findJobExecutions(jobInstance).get(0);
        assertEquals(execution, updated);
        assertEquals(BatchStatus.COMPLETED, updated.getStatus());
    }

    /**
     * Check the execution with most recent start time is returned
     */
@org.junit.Test
public void testGetLastExecution() {
        JobExecution exec1 = new JobExecution(jobInstance);
        exec1.setCreateTime(new Date(0));

        ExecutionContext ctx = new ExecutionContext();
        ctx.put("key", "value");

        JobExecution exec2 = new JobExecution(jobInstance);
        exec2.setExecutionContext(ctx);
        exec2.setCreateTime(new Date(1));

        dao.saveJobExecution(exec1);
        dao.saveJobExecution(exec2);
        dao.saveOrUpdateExecutionContext(exec2);

        JobExecution last = dao.getLastJobExecution(jobInstance);
        assertEquals(exec2, last);
        assertEquals("value", last.getExecutionContext().getString("key"));
    }

@org.junit.Test
public void testSaveAndFindContext() {
        dao.saveJobExecution(execution);
        ExecutionContext ctx = new ExecutionContext();
        ctx.put("key", "value");
        execution.setExecutionContext(ctx);
        dao.saveOrUpdateExecutionContext(execution);

        ExecutionContext retrieved = dao.findExecutionContext(execution);
        assertEquals(ctx, retrieved);
    }

@org.junit.Test
public void testSaveAndFindEmptyContext() {
        dao.saveJobExecution(execution);
        ExecutionContext ctx = new ExecutionContext();
        execution.setExecutionContext(ctx);
        dao.saveOrUpdateExecutionContext(execution);

        ExecutionContext retrieved = dao.findExecutionContext(execution);
        assertEquals(ctx, retrieved);
    }

@org.junit.Test
public void testUpdateContext() {
        dao.saveJobExecution(execution);
        ExecutionContext ctx = new ExecutionContext();
        ctx.put("key", "value");
        execution.setExecutionContext(ctx);
        dao.saveOrUpdateExecutionContext(execution);

        ctx.putLong("longKey", 7);
        dao.saveOrUpdateExecutionContext(execution);

        ExecutionContext retrieved = dao.findExecutionContext(execution);
        assertEquals(ctx, retrieved);
        assertEquals(7, retrieved.getLong("longKey"));
    }
}
