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

package org.springframework.batch.repeat.support;

import static org.junit.Assert.*;

import org.springframework.batch.repeat.RepeatContext;
import org.springframework.batch.repeat.context.RepeatContextSupport;
import org.springframework.batch.repeat.support.RepeatSynchronizationManager;

public class RepeatSynchronizationManagerTests {

	private RepeatContext context = new RepeatContextSupport(null);

	    @org.junit.Before
public void setUp() throws Exception {
		RepeatSynchronizationManager.clear();
	}
	
	@org.junit.After
    public void tearDown() throws Exception {
		RepeatSynchronizationManager.clear();
	}

	@org.junit.Test
public void testGetContext() {
		RepeatSynchronizationManager.register(context);
		assertEquals(context, RepeatSynchronizationManager.getContext());
	}

	@org.junit.Test
public void testSetSessionCompleteOnly() {
		assertNull(RepeatSynchronizationManager.getContext());
		RepeatSynchronizationManager.register(context);
		assertFalse(RepeatSynchronizationManager.getContext().isCompleteOnly());
		RepeatSynchronizationManager.setCompleteOnly();
		assertTrue(RepeatSynchronizationManager.getContext().isCompleteOnly());
	}

	@org.junit.Test
public void testSetSessionCompleteOnlyWithParent() {
		assertNull(RepeatSynchronizationManager.getContext());
		RepeatContext child = new RepeatContextSupport(context);
		RepeatSynchronizationManager.register(child);
		assertFalse(child.isCompleteOnly());
		RepeatSynchronizationManager.setAncestorsCompleteOnly();
		assertTrue(child.isCompleteOnly());
		assertTrue(context.isCompleteOnly());
	}

	@org.junit.Test
public void testClear() {
		RepeatSynchronizationManager.register(context);
		assertEquals(context, RepeatSynchronizationManager.getContext());
		RepeatSynchronizationManager.clear();
		assertEquals(null, RepeatSynchronizationManager.getContext());
	}

}
