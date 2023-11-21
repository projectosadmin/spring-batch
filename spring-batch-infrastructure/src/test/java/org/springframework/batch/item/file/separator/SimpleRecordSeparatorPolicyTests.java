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

package org.springframework.batch.item.file.separator;

import static org.junit.Assert.*;

public class SimpleRecordSeparatorPolicyTests {

	SimpleRecordSeparatorPolicy policy = new SimpleRecordSeparatorPolicy();
	
	@org.junit.Test
public void testNormalLine() throws Exception {
		assertTrue(policy.isEndOfRecord("a string"));
	}

	@org.junit.Test
public void testEmptyLine() throws Exception {
		assertTrue(policy.isEndOfRecord(""));
	}

	@org.junit.Test
public void testNullLine() throws Exception {
		assertTrue(policy.isEndOfRecord(null));
	}
	
	@org.junit.Test
public void testPostProcess() throws Exception {
		String line = "foo\nbar";
		assertEquals(line, policy.postProcess(line));
	}

	@org.junit.Test
public void testPreProcess() throws Exception {
		String line = "foo\nbar";
		assertEquals(line, policy.preProcess(line));
	}
}
