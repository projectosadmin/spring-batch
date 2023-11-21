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
package org.springframework.batch.sample.item.writer;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;

import static org.junit.Assert.*;

import org.springframework.batch.sample.AbstractDaoTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.ClassUtils;

@ContextConfiguration("staging-test-context.xml")
public class StagingItemWriterTests extends AbstractDaoTest {

    private StagingItemWriter writer;

    public void setWriter(StagingItemWriter processor) {
        this.writer = processor;
    }

    protected String[] getConfigLocations() {
        return new String[]{ClassUtils.addResourcePathToPackagePath(StagingItemWriter.class,
                "staging-test-context.xml")};
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.test.AbstractTransactionalSpringContextTests#onSetUpBeforeTransaction()
     */
    @org.junit.Before
    public void onSetUpBeforeTransaction() throws Exception {
        StepExecution stepExecution = new StepExecution("stepName", new JobExecution(new JobInstance(new Long(12L),
                new JobParameters(), "testJob")));
        writer.beforeStep(stepExecution);
    }

    @org.junit.Test
    public void testProcessInsertsNewItem() throws Exception {
        int before = getJdbcTemplate().queryForObject("SELECT COUNT(*) from BATCH_STAGING", Integer.class);
        writer.write("FOO");
        int after = getJdbcTemplate().queryForObject("SELECT COUNT(*) from BATCH_STAGING", Integer.class);
        assertEquals(before + 1, after);
    }

}
