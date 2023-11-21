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
package org.springframework.batch.core;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

import org.apache.commons.lang.SerializationUtils;
import org.springframework.batch.core.step.StepSupport;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.ExitStatus;
import org.springframework.batch.support.PropertiesConverter;

/**
 * @author Dave Syer
 * 
 */
public class StepExecutionTests {

	private StepExecution execution = newStepExecution(new StepSupport("stepName"), new Long(23));

	private StepExecution blankExecution = newStepExecution(new StepSupport("blank"), null);

	@org.junit.Test
public void testStepExecution() {
		assertNull(new StepExecution("step", null).getId());
	}

	@org.junit.Test
public void testStepExecutionWithNullId() {
		assertNull(new StepExecution("stepName", new JobExecution(new JobInstance(null,null,"foo"))).getId());
	}

	/**
	 * Test method for
	 * {@link org.springframework.batch.core.JobExecution#getEndTime()}.
	 */
	@org.junit.Test
public void testGetEndTime() {
		assertNull(execution.getEndTime());
		execution.setEndTime(new Date(0L));
		assertEquals(0L, execution.getEndTime().getTime());
	}

	/**
	 * Test method for
	 * {@link org.springframework.batch.core.JobExecution#getStartTime()}.
	 */
	@org.junit.Test
public void testGetStartTime() {
		assertNotNull(execution.getStartTime());
		execution.setStartTime(new Date(10L));
		assertEquals(10L, execution.getStartTime().getTime());
	}

	/**
	 * Test method for
	 * {@link org.springframework.batch.core.JobExecution#getStatus()}.
	 */
	@org.junit.Test
public void testGetStatus() {
		assertEquals(BatchStatus.STARTING, execution.getStatus());
		execution.setStatus(BatchStatus.COMPLETED);
		assertEquals(BatchStatus.COMPLETED, execution.getStatus());
	}

	/**
	 * Test method for
	 * {@link org.springframework.batch.core.JobExecution#getJobId()}.
	 */
	@org.junit.Test
public void testGetJobId() {
		assertEquals(23, execution.getJobExecutionId().longValue());
	}

	/**
	 * Test method for
	 * {@link org.springframework.batch.core.JobExecution#getExitStatus()}.
	 */
	@org.junit.Test
public void testGetExitCode() {
		assertEquals(ExitStatus.CONTINUABLE, execution.getExitStatus());
		execution.setExitStatus(ExitStatus.FINISHED);
		assertEquals(ExitStatus.FINISHED, execution.getExitStatus());
	}

	/**
	 * Test method for
	 * {@link org.springframework.batch.core.StepExecution#getCommitCount()}.
	 */
	@org.junit.Test
public void testGetCommitCount() {
		execution.setCommitCount(123);
		assertEquals(123, execution.getCommitCount().intValue());
	}

	/**
	 * Test method for
	 * {@link org.springframework.batch.core.StepExecution#getItemCount()}.
	 */
	@org.junit.Test
public void testGetTaskCount() {
		execution.setItemCount(123);
		assertEquals(123, execution.getItemCount().intValue());
	}

	@org.junit.Test
public void testGetJobExecution() throws Exception {
		assertNotNull(execution.getJobExecution());
	}

	@org.junit.Test
public void testApplyContribution() throws Exception {
		StepContribution contribution = execution.createStepContribution();
		contribution.incrementCommitCount();
		execution.apply(contribution);
		assertEquals(new Integer(1), execution.getCommitCount());
	}

	@org.junit.Test
public void testTerminateOnly() throws Exception {
		assertFalse(execution.isTerminateOnly());
		execution.setTerminateOnly();
		assertTrue(execution.isTerminateOnly());
	}

	@org.junit.Test
public void testNullNameIsIllegal() throws Exception {
		try {
			new StepExecution(null, new JobExecution(new JobInstance(null, null, "job")));
			fail();
		}
		catch (IllegalArgumentException e) {
			// expected
		}
	}

	@org.junit.Test
public void testToString() throws Exception {
		assertTrue("Should contain item count: " + execution.toString(), execution.toString().indexOf("item") >= 0);
		assertTrue("Should contain commit count: " + execution.toString(), execution.toString().indexOf("commit") >= 0);
		assertTrue("Should contain rollback count: " + execution.toString(),
				execution.toString().indexOf("rollback") >= 0);
	}

	@org.junit.Test
public void testExecutionContext() throws Exception {
		assertNotNull(execution.getExecutionContext());
		ExecutionContext context = new ExecutionContext();
		context.putString("foo", "bar");
		execution.setExecutionContext(context);
		assertEquals("bar", execution.getExecutionContext().getString("foo"));
	}

	@org.junit.Test
public void testEqualsWithSameIdentifier() throws Exception {
		Step step = new StepSupport("stepName");
		Entity stepExecution1 = newStepExecution(step, new Long(11));
		Entity stepExecution2 = newStepExecution(step, new Long(11));
		assertEquals(stepExecution1, stepExecution2);
	}

	@org.junit.Test
public void testEqualsWithNull() throws Exception {
		Entity stepExecution = newStepExecution(new StepSupport("stepName"), new Long(11));
		assertFalse(stepExecution.equals(null));
	}

	@org.junit.Test
public void testEqualsWithNullIdentifiers() throws Exception {
		Entity stepExecution = newStepExecution(new StepSupport("stepName"), new Long(11));
		assertFalse(stepExecution.equals(blankExecution));
	}

	@org.junit.Test
public void testEqualsWithNullJob() throws Exception {
		Entity stepExecution = newStepExecution(new StepSupport("stepName"), new Long(11));
		assertFalse(stepExecution.equals(blankExecution));
	}

	@org.junit.Test
public void testEqualsWithSelf() throws Exception {
		assertTrue(execution.equals(execution));
	}

	@org.junit.Test
public void testEqualsWithDifferent() throws Exception {
		Entity stepExecution = newStepExecution(new StepSupport("foo"), new Long(13));
		assertFalse(execution.equals(stepExecution));
	}

	@org.junit.Test
public void testEqualsWithNullStepId() throws Exception {
		Step step = new StepSupport("name");
		execution = newStepExecution(step, new Long(31));
		assertEquals("name", execution.getStepName());
		StepExecution stepExecution = newStepExecution(step, new Long(31));
		assertEquals(stepExecution.getJobExecutionId(), execution.getJobExecutionId());
		assertTrue(execution.equals(stepExecution));
	}

	@org.junit.Test
public void testHashCode() throws Exception {
		assertTrue("Hash code same as parent", new Entity(execution.getId()).hashCode() != execution.hashCode());
	}

	@org.junit.Test
public void testHashCodeWithNullIds() throws Exception {
		assertTrue("Hash code not same as parent", new Entity(execution.getId()).hashCode() != blankExecution
				.hashCode());
	}

	@org.junit.Test
public void testHashCodeViaHashSet() throws Exception {
		Set set = new HashSet();
		set.add(execution);
		assertTrue(set.contains(execution));
		execution.setExecutionContext(new ExecutionContext(PropertiesConverter.stringToProperties("foo=bar")));
		assertTrue(set.contains(execution));
	}
	
	@org.junit.Test
public void testSerialization() throws Exception {
		
		ExitStatus status = ExitStatus.NOOP;
		execution.setExitStatus(status);
		execution.setExecutionContext(new ExecutionContext(PropertiesConverter.stringToProperties("foo=bar")));
		
		byte[] serialized = SerializationUtils.serialize(execution);
		StepExecution deserialized = (StepExecution) SerializationUtils.deserialize(serialized);
		
		assertEquals(execution, deserialized);
		assertEquals(status, deserialized.getExitStatus());
	}

	private StepExecution newStepExecution(Step step, Long long2) {
		JobInstance job = new JobInstance(new Long(3), new JobParameters(), "testJob");
		StepExecution execution = new StepExecution(step.getName(), new JobExecution(job, long2), new Long(4));
		return execution;
	}

}
