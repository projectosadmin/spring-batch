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

package org.springframework.batch.repeat.policy;

import static org.junit.Assert.*;

import org.springframework.batch.repeat.ExitStatus;
import org.springframework.batch.repeat.RepeatContext;

public class TimeoutCompletionPolicyTests {

	@org.junit.Test
public void testSimpleTimeout() throws Exception {
		TimeoutTerminationPolicy policy = new TimeoutTerminationPolicy(20L);
		RepeatContext context = policy.start(null);
		assertFalse(policy.isComplete(context));
		Thread.sleep(50L);
		assertTrue(policy.isComplete(context));
	}

	@org.junit.Test
public void testSuccessfulResult() throws Exception {
		TimeoutTerminationPolicy policy = new TimeoutTerminationPolicy();
		RepeatContext context = policy.start(null);
		assertFalse(policy.isComplete(context, null));
	}
	
	@org.junit.Test
public void testNonContinuableResult() throws Exception {
		TimeoutTerminationPolicy policy = new TimeoutTerminationPolicy();
		ExitStatus result = new ExitStatus(false, "non-continuable exit status");
		assertTrue(policy.isComplete(policy.start(null), result));
	}

	@org.junit.Test
public void testUpdate() throws Exception {
		TimeoutTerminationPolicy policy = new TimeoutTerminationPolicy(20L);
		RepeatContext context = policy.start(null);
		assertFalse(policy.isComplete(context));
		Thread.sleep(50L);
		assertTrue(policy.isComplete(context));
		policy.update(context);
		// update doesn't change completeness
		assertTrue(policy.isComplete(context));
	}

}
