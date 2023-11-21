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

package org.springframework.batch.core.repository.support;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

import org.easymock.MockControl;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.JobSupport;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.core.step.StepSupport;

/**
 * Test SimpleJobRepository. The majority of test cases are tested using
 * EasyMock, however, there were some issues with using it for the stepExecutionDao when
 * testing finding or creating steps, so an actual mock class had to be written.
 * 
 * @author Lucas Ward
 * 
 */
public class SimpleJobRepositoryTests {

	SimpleJobRepository jobRepository;

	JobSupport job;

	JobParameters jobParameters;

	Step stepConfiguration1;

	Step stepConfiguration2;

	MockControl jobExecutionDaoControl = MockControl.createControl(JobExecutionDao.class);
	
	MockControl jobInstanceDaoControl = MockControl.createControl(JobInstanceDao.class);

	MockControl stepExecutionDaoControl = MockControl.createControl(StepExecutionDao.class);
	
	JobExecutionDao jobExecutionDao;
	
	JobInstanceDao jobInstanceDao;

	StepExecutionDao stepExecutionDao;

	JobInstance databaseJob;

	String databaseStep1;

	String databaseStep2;

	List steps;


	    @org.junit.Before
public void setUp() throws Exception {

		jobExecutionDao = (JobExecutionDao) jobExecutionDaoControl.getMock();
		jobInstanceDao = (JobInstanceDao) jobInstanceDaoControl.getMock();
		stepExecutionDao = (StepExecutionDao) stepExecutionDaoControl.getMock();

		jobRepository = new SimpleJobRepository(jobInstanceDao, jobExecutionDao, stepExecutionDao);

		jobParameters = new JobParametersBuilder().toJobParameters();

		job = new JobSupport();
		job.setBeanName("RepositoryTest");
		job.setRestartable(true);

		stepConfiguration1 = new StepSupport("TestStep1");

		stepConfiguration2 = new StepSupport("TestStep2");

		List stepConfigurations = new ArrayList();
		stepConfigurations.add(stepConfiguration1);
		stepConfigurations.add(stepConfiguration2);

		job.setSteps(stepConfigurations);

		databaseJob = new JobInstance(new Long(1), jobParameters, job.getName());

		databaseStep1 = "dbStep1";
		databaseStep2 = "dbStep2";

		steps = new ArrayList();
		steps.add(databaseStep1);
		steps.add(databaseStep2);

	}


	@org.junit.Test
public void testSaveOrUpdateInvalidJobExecution() {

		// failure scenario - must have job ID
		JobExecution jobExecution = new JobExecution(null);
		try {
			jobRepository.saveOrUpdate(jobExecution);
			fail();
		}
		catch (Exception ex) {
			// expected
		}
	}

	@org.junit.Test
public void testSaveOrUpdateValidJobExecution() throws Exception {

		JobExecution jobExecution = new JobExecution(new JobInstance(new Long(1), jobParameters, job.getName()));

		// new execution - call save on job dao
		jobExecutionDao.saveJobExecution(jobExecution);
		jobExecutionDaoControl.replay();
		jobRepository.saveOrUpdate(jobExecution);
		jobExecutionDaoControl.reset();

		// update existing execution
		jobExecution.setId(new Long(5));
		jobExecutionDao.updateJobExecution(jobExecution);
		jobExecutionDaoControl.replay();
		jobRepository.saveOrUpdate(jobExecution);
	}

	@org.junit.Test
public void testSaveOrUpdateStepExecutionException() {

		StepExecution stepExecution = new StepExecution("stepName", null, null);

		// failure scenario -- no step id set.
		try {
			jobRepository.saveOrUpdate(stepExecution);
			fail();
		}
		catch (Exception ex) {
			// expected
		}
	}

}
