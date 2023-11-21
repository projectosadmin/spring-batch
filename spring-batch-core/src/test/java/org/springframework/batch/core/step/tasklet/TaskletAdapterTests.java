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
package org.springframework.batch.core.step.tasklet;

import org.springframework.batch.core.step.tasklet.TaskletAdapter;
import org.springframework.batch.repeat.ExitStatus;

import static org.junit.Assert.*;

/**
 * @author Dave Syer
 *
 */
public class TaskletAdapterTests {
	
	private TaskletAdapter tasklet = new TaskletAdapter();
	private Object result = null;
	
	public ExitStatus execute() {
		return ExitStatus.NOOP;
	}

	public Object process() {
		return result ;
	}

	/* (non-Javadoc) 
	 * @see junit.framework.TestCase#setUp()
	 */
	    @org.junit.Before
public void setUp() throws Exception {
		tasklet.setTargetObject(this);
		tasklet.setTargetMethod("execute");
	}

	/**
	 * Test method for {@link org.springframework.batch.core.step.tasklet.TaskletAdapter#execute()}.
	 * @throws Exception 
	 */
	@org.junit.Test
public void testExecuteWithExitStatus() throws Exception {
		assertEquals(ExitStatus.NOOP, tasklet.execute());
	}

	/**
	 * Test method for {@link org.springframework.batch.core.step.tasklet.TaskletAdapter#mapResult(java.lang.Object)}.
	 */
	@org.junit.Test
public void testMapResultWithNull() throws Exception {
		tasklet.setTargetMethod("process");
		assertEquals(ExitStatus.FINISHED, tasklet.execute());
	}

	/**
	 * Test method for {@link org.springframework.batch.core.step.tasklet.TaskletAdapter#mapResult(java.lang.Object)}.
	 */
	@org.junit.Test
public void testMapResultWithNonNull() throws Exception {
		tasklet.setTargetMethod("process");
		this.result = "foo";
		assertEquals(ExitStatus.FINISHED, tasklet.execute());
	}

}
