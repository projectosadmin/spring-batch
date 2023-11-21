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

package org.springframework.batch.core.resource;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

import org.junit.Before;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.step.StepSupport;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.DescriptiveResource;
import org.springframework.core.io.Resource;

/**
 * Unit tests for {@link StepExecutionResourceProxy}
 * 
 * @author robert.kasanicky
 * @author Lucas Ward
 * @author Dave Syer
 */
public class StepExecutionResourceProxyTests {

	/**
	 * Object under test
	 */
	private StepExecutionResourceProxy resource = new StepExecutionResourceProxy();

	private char pathsep = File.separatorChar;

	private String path = "data" + pathsep;

	private JobInstance jobInstance;

	private StepExecution stepExecution;

	/**
	 * mock step context
	 */

	    @org.junit.Before
public void setUp() throws Exception {

		jobInstance = new JobInstance(new Long(0), new JobParameters(), "testJob");
		JobExecution jobExecution = new JobExecution(jobInstance);
		Step step = new StepSupport("bar");
		stepExecution = jobExecution.createStepExecution(step);
		resource.beforeStep(stepExecution);

	}

	/**
	 * regular use with valid context and pattern provided
	 */
	@org.junit.Test
public void testCreateFileName() throws Exception {
		doTestPathName("bar.txt", path);
	}

	@org.junit.Test
public void testNullFilePattern() throws Exception {
		resource.setFilePattern(null);
		try {
			resource.beforeStep(stepExecution);
			fail("Expected IllegalArgumentException");
		}
		catch (IllegalArgumentException e) {
			// expected
		}
	}

	@org.junit.Test
public void testNonStandardFilePattern() throws Exception {
		resource.setFilePattern("foo/data/%JOB_NAME%/" + "%STEP_NAME%-job");
		resource.beforeStep(stepExecution);
		doTestPathName("bar-job", "foo" + pathsep + "data" + pathsep);
	}

	@org.junit.Test
public void testNonStandardFilePatternWithJobParameters() throws Exception {
		resource.setFilePattern("foo/data/%JOB_NAME%/%job.key%-foo");
		jobInstance = new JobInstance(new Long(0), new JobParametersBuilder().addString("job.key", "spam")
				.toJobParameters(), "testJob");
		JobExecution jobExecution = new JobExecution(jobInstance);
		Step step = new StepSupport("bar");
		resource.beforeStep(jobExecution.createStepExecution(step));
		doTestPathName("spam-foo", "foo" + pathsep + "data" + pathsep);
	}

	@org.junit.Test
public void testResoureLoaderAware() throws Exception {
		resource = new StepExecutionResourceProxy();
		resource.setResourceLoader(new DefaultResourceLoader() {
			public Resource getResource(String location) {
				return new ByteArrayResource("foo".getBytes());
			}
		});
		resource.beforeStep(stepExecution);
		assertTrue(resource.exists());
	}
	
	/**
	 * toString delegates to the proxied resource.
	 */
	@org.junit.Test
public void testToString() {
		resource = new StepExecutionResourceProxy();
		resource.setResourceLoader(new DefaultResourceLoader() {
			public Resource getResource(String location) {
				return new DescriptiveResource("toStringTestResource") {
					public String toString() {
						return "to-string-test-resource";
					}
				};
			}
		});
		resource.beforeStep(stepExecution);
		assertEquals("to-string-test-resource", resource.toString());
	}
	
	/**
	 * If delegate is not set toString returns the filePattern.
	 */
	@org.junit.Test
public void testToStringWithNullDelegate() {
		resource = new StepExecutionResourceProxy();
		String filePattern = "arbitrary pattern";
		resource.setFilePattern("arbitrary pattern");
		assertEquals(filePattern, resource.toString());
	}
	
	@org.junit.Test
public void testNonExistentJobParameter() throws Exception{
		
		resource.setFilePattern("foo/data/%JOB_NAME%/%non.key%-foo");
		jobInstance = new JobInstance(new Long(0), new JobParametersBuilder().addString("job.key", "spam")
				.toJobParameters(), "testJob");
		JobExecution jobExecution = new JobExecution(jobInstance);
		Step step = new StepSupport("bar");
		try{
			resource.beforeStep(jobExecution.createStepExecution(step));
			fail();
		}
		catch(Exception ex){
			//expected, if there isn't a JobParameter for that key, it should throw an exception
		}
	}
	
	@org.junit.Test
public void testLongJobParameter() throws Exception {
		
		resource.setFilePattern("foo/data/%JOB_NAME%/%job.key%-foo");
		jobInstance = new JobInstance(new Long(0), new JobParametersBuilder().addLong("job.key", new Long(123))
				.toJobParameters(), "testJob");
		JobExecution jobExecution = new JobExecution(jobInstance);
		Step step = new StepSupport("bar");
		resource.beforeStep(jobExecution.createStepExecution(step));
		doTestPathName("123-foo", "foo" + pathsep + "data" + pathsep);
	}

	private void doTestPathName(String filename, String path) throws Exception, IOException {
		String returnedPath = resource.getFile().getAbsolutePath();
		String absolutePath = new File(path + jobInstance.getJobName() + pathsep + filename).getAbsolutePath();
		assertEquals(absolutePath, returnedPath);
	}

}
