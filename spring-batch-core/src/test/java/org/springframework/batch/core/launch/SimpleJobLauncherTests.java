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

package org.springframework.batch.core.launch;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

import org.easymock.MockControl;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.JobSupport;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.repeat.ExitStatus;
import org.springframework.core.task.TaskExecutor;

/**
 * @author Lucas Ward
 * 
 */
public class SimpleJobLauncherTests {

	private SimpleJobLauncher jobLauncher;

	private MockControl repositoryControl = MockControl.createControl(JobRepository.class);

	private Job job = new JobSupport("foo") {
		public void execute(JobExecution execution) {
			execution.setExitStatus(ExitStatus.FINISHED);
			return;
		}
	};

	private JobParameters jobParameters = new JobParameters();

	private JobRepository jobRepository;

	    @org.junit.Before
public void setUp() throws Exception {
		

		jobLauncher = new SimpleJobLauncher();
		jobRepository = (JobRepository) repositoryControl.getMock();
		jobLauncher.setJobRepository(jobRepository);

	}

	@org.junit.Test
public void testRun() throws Exception {

		JobExecution jobExecution = new JobExecution(null);

		jobRepository.createJobExecution(job, jobParameters);
		repositoryControl.setReturnValue(jobExecution);

		repositoryControl.replay();

		jobLauncher.afterPropertiesSet();
		jobLauncher.run(job, jobParameters);
		assertEquals(ExitStatus.FINISHED, jobExecution.getExitStatus());

		repositoryControl.verify();
	}

	@org.junit.Test
public void testTaskExecutor() throws Exception {
		final List list = new ArrayList();
		jobLauncher.setTaskExecutor(new TaskExecutor() {
			public void execute(Runnable task) {
				list.add("execute");
				task.run();
			}
		});
		testRun();
		assertEquals(1, list.size());
	}

	@org.junit.Test
public void testRunWithException() throws Exception {
		job = new JobSupport() {
			public void execute(JobExecution execution) {
				execution.setExitStatus(ExitStatus.FAILED);
				throw new RuntimeException("foo");
			}
		};
		try {
			testRun();
			fail("Expected RuntimeException");
		} catch (RuntimeException e) {
			assertEquals("foo", e.getMessage());
		}
	}

	@org.junit.Test
public void testRunWithError() throws Exception {
		job = new JobSupport() {
			public void execute(JobExecution execution) {
				execution.setExitStatus(ExitStatus.FAILED);
				throw new Error("foo");
			}
		};
		try {
			testRun();
			fail("Expected Error");
		} catch (RuntimeException e) {
			assertEquals("foo", e.getCause().getMessage());
		}
	}

	@org.junit.Test
public void testInitialiseWithoutRepository() throws Exception {
		try {
			new SimpleJobLauncher().afterPropertiesSet();
			fail("Expected IllegalArgumentException");
		} catch (IllegalStateException e) {
			// expected
			assertTrue("Message did not contain repository: " + e.getMessage(), contains(e.getMessage().toLowerCase(),
			        "repository"));
		}
	}

	@org.junit.Test
public void testInitialiseWithRepository() throws Exception {
		jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.afterPropertiesSet(); // no error
	}

	private boolean contains(String str, String searchStr) {
		return str.indexOf(searchStr) != -1;
	}
}