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

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.EmptyItemWriter;
import org.springframework.batch.core.step.JobRepositorySupport;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.ExitStatus;
import org.springframework.batch.repeat.RepeatCallback;
import org.springframework.batch.repeat.RepeatOperations;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;

/**
 * @author Dave Syer
 * 
 */
public class RepeatOperationsStepFactoryBeanTests {

	private RepeatOperationsStepFactoryBean factory = new RepeatOperationsStepFactoryBean();

	private List list;

	private JobExecution jobExecution = new JobExecution(new JobInstance(new Long(0L), new JobParameters(), "job"));

	    @org.junit.Before
public void setUp() throws Exception {
		factory.setBeanName("RepeatOperationsStep");
		factory.setItemReader(new ListItemReader(new ArrayList()));
		factory.setItemWriter(new EmptyItemWriter());
		factory.setJobRepository(new JobRepositorySupport());
		factory.setTransactionManager(new ResourcelessTransactionManager());
	}

	@org.junit.Test
public void testType() throws Exception {
		assertEquals(Step.class, factory.getObjectType());
	}

	@org.junit.Test
public void testDefaultValue() throws Exception {
		assertTrue(factory.getObject() instanceof Step);
	}

	@org.junit.Test
public void testStepOperationsWithoutChunkListener() throws Exception {

		factory.setItemReader(new ListItemReader(new ArrayList()));
		factory.setItemWriter(new EmptyItemWriter());
		factory.setJobRepository(new JobRepositorySupport());
		factory.setTransactionManager(new ResourcelessTransactionManager());

		factory.setStepOperations(new RepeatOperations() {

			public ExitStatus iterate(RepeatCallback callback) {
				list = new ArrayList();
				list.add("foo");
				return ExitStatus.FINISHED;
			}
		});

		Step step = (Step) factory.getObject();
		step.execute(new StepExecution(step.getName(), jobExecution));

		assertEquals(1, list.size());
	}

}
