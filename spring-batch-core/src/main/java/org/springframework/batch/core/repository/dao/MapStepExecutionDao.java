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

import java.util.Map;

import org.apache.commons.lang.SerializationUtils;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.support.transaction.TransactionAwareProxyFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.util.Assert;

/**
 * In-memory implementation of {@link StepExecutionDao}.
 */
public class MapStepExecutionDao implements StepExecutionDao {

	private static final Map<Long, Map<String, StepExecution>> executionsByJobExecutionId;

	private static final Map<Long, ExecutionContext> contextsByStepExecutionId;

	private static long currentId = 0;

	static {
		executionsByJobExecutionId = TransactionAwareProxyFactory.createTransactionalMap();
		contextsByStepExecutionId = TransactionAwareProxyFactory.createTransactionalMap();
	}

	public static void clear() {
		executionsByJobExecutionId.clear();
		contextsByStepExecutionId.clear();
	}
	
	private static StepExecution copy(StepExecution original) {
		return (StepExecution) SerializationUtils.deserialize(SerializationUtils.serialize(original));
	}
	
	private static ExecutionContext copy(ExecutionContext original) {
		return (ExecutionContext) SerializationUtils.deserialize(SerializationUtils.serialize(original));
	}

	public ExecutionContext findExecutionContext(StepExecution stepExecution) {
		return copy(contextsByStepExecutionId.get(stepExecution.getId()));
	}

	public void saveStepExecution(StepExecution stepExecution) {
		Assert.isTrue(stepExecution.getId() == null, "stepExecution.id");
		Assert.isTrue(stepExecution.getVersion() == null, "stepExecution.version");
		Assert.notNull(stepExecution.getJobExecutionId(), "JobExecution must be saved already.");

		Map<String, StepExecution> executions = executionsByJobExecutionId.get(stepExecution.getJobExecutionId());
		if (executions == null) {
			executions = TransactionAwareProxyFactory.createTransactionalMap();
			executionsByJobExecutionId.put(stepExecution.getJobExecutionId(), executions);
		}
		stepExecution.setId(currentId++);
		stepExecution.incrementVersion();
		executions.put(stepExecution.getStepName(), copy(stepExecution));
	}

	public void updateStepExecution(StepExecution stepExecution) {

		Assert.notNull(stepExecution.getJobExecutionId(), "stepExecution.getJobExecutionId");

		Map<String, StepExecution> executions = executionsByJobExecutionId.get(stepExecution.getJobExecutionId());
		Assert.notNull(executions, "step executions for given job execution are expected to be already saved");

		StepExecution persistedExecution = executions.get(stepExecution.getStepName());
		Assert.notNull(persistedExecution, "step execution is expected to be already saved");

		synchronized (stepExecution) {
			if (!persistedExecution.getVersion().equals(stepExecution.getVersion())) {
				throw new OptimisticLockingFailureException("Attempt to update step execution id="
						+ stepExecution.getId() + " with wrong version (" + stepExecution.getVersion()
						+ "), where current version is " + persistedExecution.getVersion());
			}

			stepExecution.incrementVersion();
			executions.put(stepExecution.getStepName(), copy(stepExecution));
		}
	}

	public StepExecution getStepExecution(JobExecution jobExecution, Step step) {
		Map<String, StepExecution> executions = executionsByJobExecutionId.get(jobExecution.getId());
		if (executions == null) {
			return null;
		}

		return copy(executions.get(step.getName()));
	}

	public void saveOrUpdateExecutionContext(StepExecution stepExecution) {
		contextsByStepExecutionId.put(stepExecution.getId(), copy(stepExecution.getExecutionContext()));
	}

}
