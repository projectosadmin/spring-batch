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

package org.springframework.batch.core.launch.support;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

import org.springframework.batch.core.launch.support.ExitCodeMapper;
import org.springframework.batch.core.launch.support.SimpleJvmExitCodeMapper;
import org.springframework.batch.repeat.ExitStatus;

public class SimpleJvmExitCodeMapperTests {

	private SimpleJvmExitCodeMapper ecm;
	private SimpleJvmExitCodeMapper ecm2;
	
	    @org.junit.Before
public void setUp() throws Exception {
		ecm = new SimpleJvmExitCodeMapper();
		Map ecmMap = new HashMap();
		ecmMap.put("MY_CUSTOM_CODE", new Integer(3));
		ecm.setMapping(ecmMap);
		
		ecm2 = new SimpleJvmExitCodeMapper();
		Map ecm2Map = new HashMap();
		ecm2Map.put(ExitStatus.FINISHED.getExitCode(), new Integer(-1));
		ecm2Map.put(ExitStatus.FAILED.getExitCode(), new Integer(-2));
		ecm2Map.put(ExitCodeMapper.JOB_NOT_PROVIDED, new Integer(-3));
		ecm2Map.put(ExitCodeMapper.NO_SUCH_JOB, new Integer(-3));
		ecm2.setMapping(ecm2Map);
	}

	@org.junit.After
    public void tearDown() throws Exception {
		
	}

	@org.junit.Test
public void testGetExitCodeWithpPredefinedCodes() {
		assertEquals(
				ecm.intValue(ExitStatus.FINISHED.getExitCode()),
				ExitCodeMapper.JVM_EXITCODE_COMPLETED);
		assertEquals(
				ecm.intValue(ExitStatus.FAILED.getExitCode()),
				ExitCodeMapper.JVM_EXITCODE_GENERIC_ERROR);
		assertEquals(
				ecm.intValue(ExitCodeMapper.JOB_NOT_PROVIDED),
				ExitCodeMapper.JVM_EXITCODE_JOB_ERROR);		
		assertEquals(
				ecm.intValue(ExitCodeMapper.NO_SUCH_JOB),
				ExitCodeMapper.JVM_EXITCODE_JOB_ERROR);		
	}
	
	@org.junit.Test
public void testGetExitCodeWithPredefinedCodesOverridden() {
		System.out.println(ecm2.intValue(ExitStatus.FINISHED.getExitCode()));
		assertEquals(
				ecm2.intValue(ExitStatus.FINISHED.getExitCode()), -1);
		assertEquals(
				ecm2.intValue(ExitStatus.FAILED.getExitCode()), -2);
		assertEquals(
				ecm2.intValue(ExitCodeMapper.JOB_NOT_PROVIDED), -3);		
		assertEquals(
				ecm2.intValue(ExitCodeMapper.NO_SUCH_JOB), -3);		
	}

	@org.junit.Test
public void testGetExitCodeWithCustomCode() {
		assertEquals(ecm.intValue("MY_CUSTOM_CODE"),3);		
	}

	@org.junit.Test
public void testGetExitCodeWithDefaultCode() {
		assertEquals(
				ecm.intValue("UNDEFINED_CUSTOM_CODE"),
				ExitCodeMapper.JVM_EXITCODE_GENERIC_ERROR);		
	}
	
	
}
