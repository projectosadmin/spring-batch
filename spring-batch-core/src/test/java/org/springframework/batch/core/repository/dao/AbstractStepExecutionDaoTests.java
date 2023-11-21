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

package org.springframework.batch.core.repository.dao;

import org.junit.Before;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.JobSupport;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.StepSupport;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.Assert.*;

/**
 * Tests for {@link StepExecutionDao} implementations.
 *
 * @see #getStepExecutionDao()
 */

public abstract class AbstractStepExecutionDaoTests extends AbstractDaoTest {

    protected StepExecutionDao dao;

    protected JobInstance jobInstance;

    protected JobExecution jobExecution;

    protected Step step;

    protected StepExecution stepExecution;

    protected JobRepository repository;

    /**
     * @return {@link StepExecutionDao} implementation ready for use.
     */
    protected abstract StepExecutionDao getStepExecutionDao();

    /**
     * @return {@link JobRepository} that uses the stepExecution dao.
     */
    protected abstract JobRepository getJobRepository();

    @Before
    public void init() throws Exception {
        repository = getJobRepository();
        jobExecution = repository.createJobExecution(new JobSupport("testJob"), new JobParameters());
        jobInstance = jobExecution.getJobInstance();
        step = new StepSupport("foo");
        stepExecution = new StepExecution(step.getName(), jobExecution);
        dao = getStepExecutionDao();
    }

    @org.junit.Test
    public void testSaveExecutionAssignsIdAndVersion() throws Exception {

        assertNull(stepExecution.getId());
        assertNull(stepExecution.getVersion());
        dao.saveStepExecution(stepExecution);
        assertNotNull(stepExecution.getId());
        assertNotNull(stepExecution.getVersion());
    }

    @org.junit.Test
    public void testSaveAndFindExecution() {

        stepExecution.setStatus(BatchStatus.STARTED);
        stepExecution.setReadSkipCount(7);
        stepExecution.setWriteSkipCount(5);
        stepExecution.setRollbackCount(3);
        dao.saveStepExecution(stepExecution);

        StepExecution retrieved = dao.getStepExecution(jobExecution, step);
        assertEquals(stepExecution, retrieved);
        assertEquals(BatchStatus.STARTED, retrieved.getStatus());
        assertEquals(stepExecution.getReadSkipCount(), retrieved.getReadSkipCount());
        assertEquals(stepExecution.getWriteSkipCount(), retrieved.getWriteSkipCount());
        assertEquals(stepExecution.getRollbackCount(), retrieved.getRollbackCount());

        assertNull(dao.getStepExecution(jobExecution, new StepSupport("not-existing step")));
    }

    @org.junit.Test
    public void testGetForNotExistingJobExecution() {
        assertNull(dao.getStepExecution(new JobExecution(jobInstance, new Long(777)), step));
    }

    /**
     * To-be-saved execution must not already have an id.
     */
    @org.junit.Test
    public void testSaveExecutionWithIdAlreadySet() {
        stepExecution.setId(new Long(7));
        try {
            dao.saveStepExecution(stepExecution);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    /**
     * To-be-saved execution must not already have a version.
     */
    @org.junit.Test
    public void testSaveExecutionWithVersionAlreadySet() {
        stepExecution.incrementVersion();
        try {
            dao.saveStepExecution(stepExecution);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    /**
     * Update and retrieve updated StepExecution - make sure the update is
     * reflected as expected and version number has been incremented
     */
    @org.junit.Test
    public void testUpdateExecution() {
        stepExecution.setStatus(BatchStatus.STARTED);
        dao.saveStepExecution(stepExecution);
        Integer versionAfterSave = stepExecution.getVersion();

        stepExecution.setStatus(BatchStatus.STOPPED);
        dao.updateStepExecution(stepExecution);
        assertEquals(versionAfterSave.intValue() + 1, stepExecution.getVersion().intValue());

        StepExecution retrieved = dao.getStepExecution(jobExecution, step);
        assertEquals(stepExecution, retrieved);
        assertEquals(BatchStatus.STOPPED, retrieved.getStatus());
    }

    @org.junit.Test
    public void testSaveAndFindContext() {
        dao.saveStepExecution(stepExecution);
        ExecutionContext ctx = new ExecutionContext();
        ctx.put("key", "value");
        stepExecution.setExecutionContext(ctx);
        dao.saveOrUpdateExecutionContext(stepExecution);

        ExecutionContext retrieved = dao.findExecutionContext(stepExecution);
        assertEquals(ctx, retrieved);
    }

    @org.junit.Test
    public void testSaveAndFindEmptyContext() {
        dao.saveStepExecution(stepExecution);
        ExecutionContext ctx = new ExecutionContext();
        stepExecution.setExecutionContext(ctx);
        dao.saveOrUpdateExecutionContext(stepExecution);

        ExecutionContext retrieved = dao.findExecutionContext(stepExecution);
        assertEquals(ctx, retrieved);
    }

    @org.junit.Test
    public void testUpdateContext() {
        dao.saveStepExecution(stepExecution);
        ExecutionContext ctx = new ExecutionContext();
        ctx.put("key", "value");
        stepExecution.setExecutionContext(ctx);
        dao.saveOrUpdateExecutionContext(stepExecution);

        ctx.putLong("longKey", 7);
        dao.saveOrUpdateExecutionContext(stepExecution);

        ExecutionContext retrieved = dao.findExecutionContext(stepExecution);
        assertEquals(ctx, retrieved);
        assertEquals(7, retrieved.getLong("longKey"));
    }

    /**
     * Exception should be raised when the version of update argument doesn't
     * match the version of persisted entity.
     */
    @org.junit.Test
    public void testConcurrentModificationException() {
        step = new StepSupport("foo");

        StepExecution exec1 = new StepExecution(step.getName(), jobExecution);
        dao.saveStepExecution(exec1);

        StepExecution exec2 = new StepExecution(step.getName(), jobExecution);
        exec2.setId(exec1.getId());

        exec2.incrementVersion();
        assertEquals(new Integer(0), exec1.getVersion());
        assertEquals(exec1.getVersion(), exec2.getVersion());

        dao.updateStepExecution(exec1);
        assertEquals(new Integer(1), exec1.getVersion());

        try {
            dao.updateStepExecution(exec2);
            fail();
        } catch (OptimisticLockingFailureException e) {
            // expected
        }

    }

    @org.junit.Test
    public void testStoreInteger() {
        dao.saveStepExecution(stepExecution);
        ExecutionContext ec = new ExecutionContext();
        ec.put("intValue", new Integer(343232));
        stepExecution.setExecutionContext(ec);
        dao.saveOrUpdateExecutionContext(stepExecution);
        ExecutionContext restoredEc = dao.findExecutionContext(stepExecution);
        assertEquals(ec, restoredEc);
    }

}
