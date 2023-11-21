package org.springframework.batch.core.repository.dao;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.item.ExecutionContext;

import java.util.List;

/**
 * Data Access Object for job executions.
 *
 * @author Lucas Ward
 * @author Robert Kasanicky
 */
public interface JobExecutionDao {

    /**
     * Save a new JobExecution.
     * Preconditions: jobInstance the jobExecution belongs to must have a jobInstanceId.
     *
     * @param jobExecution jobExecution
     */
    void saveJobExecution(JobExecution jobExecution);

    /**
     * Update and existing JobExecution.
     * <p>
     * Preconditions: jobExecution must have an Id (which can be obtained by the
     * save method) and a jobInstanceId.
     *
     * @param jobExecution jobExecution
     */
    void updateJobExecution(JobExecution jobExecution);

    /**
     * Return the number of JobExecutions for the given JobInstance
     * <p>
     * Preconditions: jobInstance must have an id.
     */
    int getJobExecutionCount(JobInstance jobInstance);

    /**
     * Return list of JobExecutions for given JobInstance.
     *
     * @param jobInstance jobInstance
     * @return list of jobExecutions.
     */
    List<JobExecution> findJobExecutions(JobInstance jobInstance);

    /**
     * @return last JobExecution for given JobInstance.
     */
    JobExecution getLastJobExecution(JobInstance jobInstance);

    /**
     * Find the {@link ExecutionContext} for the given {@link JobExecution}.
     *
     * @throws IllegalArgumentException if the id is null.
     */
    ExecutionContext findExecutionContext(JobExecution jobExecution);

    /**
     * Save the {@link ExecutionContext} of the given {@link JobExecution}.
     *
     * @param jobExecution the {@link JobExecution} containing the
     *                     {@link ExecutionContext} to be saved.
     * @throws IllegalArgumentException if the attributes are null.
     */
    void saveOrUpdateExecutionContext(JobExecution jobExecution);

}
