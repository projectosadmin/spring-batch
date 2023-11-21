/*
 * Copyright 2006-2008 the original author or authors.
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

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.dao.AbstractDaoTest;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.test.context.ContextConfiguration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Lucas Ward
 */
@ContextConfiguration({"/org/springframework/batch/core/repository/dao/data-source-context.xml"})
public class StepExecutionPreparedStatementSetterTests extends AbstractDaoTest {

    StepExecutionPreparedStatementSetter pss;
    StepExecution stepExecution;
	
/*	protected String[] getConfigLocations() {
		return new String[] { ClassUtils.addResourcePathToPackagePath(AbstractJobDaoTests.class, "data-source-context.xml") };
	}*/

    @Before
    public void onSetUpInTransaction() throws Exception {


        pss = new StepExecutionPreparedStatementSetter();
        JobParameters jobParameters = new JobParametersBuilder().addLong("begin.id", new Long(1)).addLong("end.id", new Long(4)).toJobParameters();
        JobInstance jobInstance = new JobInstance(new Long(1), jobParameters, "simpleJob");
        JobExecution jobExecution = new JobExecution(jobInstance, new Long(2));
        stepExecution = new StepExecution("taskletStep", jobExecution, new Long(3));
        pss.beforeStep(stepExecution);
        jdbcTemplate = getJdbcTemplate();
    }

    @org.junit.Test
    public void testSetValues() {

        List parameterNames = new ArrayList();
        parameterNames.add("begin.id");
        parameterNames.add("end.id");
        pss.setParameterKeys(parameterNames);
        final List results = new ArrayList();
        jdbcTemplate.query("SELECT NAME from T_FOOS where ID > ? and ID < ?", pss, new RowCallbackHandler() {

            public void processRow(ResultSet rs) throws SQLException {
                results.add(rs.getString(1));
            }
        });

        assertEquals(2, results.size());
        assertEquals("bar2", results.get(0));
        assertEquals("bar3", results.get(1));
    }

    @org.junit.Test
    public void testAfterPropertiesSet() throws Exception {
        try {
            pss.afterPropertiesSet();
            fail();
        } catch (IllegalArgumentException ex) {
            //expected
        }
    }

    @org.junit.Test
    public void testNonExistentProperties() {

        List parameterNames = new ArrayList();
        parameterNames.add("badParameter");
        parameterNames.add("end.id");
        pss.setParameterKeys(parameterNames);

        try {
            jdbcTemplate.query("SELECT NAME from T_FOOS where ID > ? and ID < ?", pss, new RowCallbackHandler() {

                public void processRow(ResultSet rs) throws SQLException {
                    fail();
                }
            });

            fail();
        } catch (IllegalStateException ex) {
            //expected
        }

    }
}
