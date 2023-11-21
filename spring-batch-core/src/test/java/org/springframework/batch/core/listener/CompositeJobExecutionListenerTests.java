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
package org.springframework.batch.core.listener;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobInstance;

/**
 * @author Dave Syer
 * 
 */
public class CompositeJobExecutionListenerTests {

	private CompositeExecutionJobListener listener = new CompositeExecutionJobListener();

	private List list = new ArrayList();

	/**
	 * Test method for
	 * {@link org.springframework.batch.core.listener.CompositeExecutionJobListener#setListeners(org.springframework.batch.core.JobExecutionListener[])}.
	 */
	@org.junit.Test
public void testSetListeners() {
		listener.setListeners(new JobExecutionListener[] { new JobExecutionListenerSupport() {
			public void afterJob(JobExecution jobExecution) {
				list.add("fail");
			}
		}, new JobExecutionListenerSupport() {
			public void afterJob(JobExecution jobExecution) {
				list.add("continue");
			}
		} });
		listener.afterJob(null);
		assertEquals(2, list.size());
	}

	/**
	 * Test method for
	 * {@link org.springframework.batch.core.listener.CompositeExecutionJobListener#register(org.springframework.batch.core.JobExecutionListener)}.
	 */
	@org.junit.Test
public void testSetListener() {
		listener.register(new JobExecutionListenerSupport() {
			public void afterJob(JobExecution jobExecution) {
				list.add("fail");
			}
		});
		listener.afterJob(null);
		assertEquals(1, list.size());
	}

	/**
	 * Test method for
	 * {@link org.springframework.batch.core.listener.CompositeExecutionJobListener#beforeJob(JobExecution)}.
	 */
	@org.junit.Test
public void testOpen() {
		listener.register(new JobExecutionListenerSupport() {
			public void beforeJob(JobExecution stepExecution) {
				list.add("foo");
			}
		});
		listener.beforeJob(new JobExecution(new JobInstance(new Long(11L), null, "testOpenJob")));
		assertEquals(1, list.size());
	}

}
