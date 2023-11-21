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

package org.springframework.batch.sample;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;

import org.springframework.util.ClassUtils;

/**
 * Abstract unit test for running functional tests by getting context locations
 * for both the container and configuration separately and having them auto
 * wired in by type. This allows the two to be completely separated, and remove
 * any 'configuration coupling' between the two.
 *
 * @author Lucas Ward
 */

public abstract class AbstractBatchLauncherTests extends AbstractDaoTest {

//	private static final String CONTAINER_DEFINITION_LOCATION = "simple-container-definition.xml";

    public AbstractBatchLauncherTests() {

    }

    /*
     * (non-Javadoc)
     * @see org.springframework.test.AbstractSingleSpringContextTests#getConfigLocations()
     */
    protected String[] getConfigLocations() {
        return new String[]{ClassUtils.addResourcePathToPackagePath(getClass(), ClassUtils.getShortName(getClass())
                + "-context.xml")};
    }

    JobLauncher launcher;
    private Job job;

    private JobParameters jobParameters = new JobParameters();

    public void setLauncher(JobLauncher bootstrap) {
        this.launcher = bootstrap;
    }

    /**
     * Public setter for the {@link Job} property.
     *
     * @param job the job to set
     */
    public void setJob(Job job) {
        this.job = job;
    }

    public Job getJob() {
        return job;
    }

    protected String getJobName() {
        return job.getName();
    }

    public void setJobParameters(JobParameters jobParameters) {
        this.jobParameters = jobParameters;
    }

    /**
     * @throws Exception Exception
     */
    @org.junit.Test
    public void testLaunchJob() throws Exception {
        launcher.run(job, jobParameters);
    }
}
