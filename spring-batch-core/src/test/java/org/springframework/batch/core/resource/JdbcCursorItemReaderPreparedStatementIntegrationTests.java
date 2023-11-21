package org.springframework.batch.core.resource;

import org.junit.Before;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.dao.AbstractDaoTest;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.test.context.ContextConfiguration;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@ContextConfiguration({"/org/springframework/batch/core/repository/dao/data-source-context.xml"})
public class JdbcCursorItemReaderPreparedStatementIntegrationTests extends
        AbstractDaoTest {

    JdbcCursorItemReader itemReader;

    @Before
    public void onSetUpInTransaction() throws Exception {

        itemReader = new JdbcCursorItemReader();
        itemReader.setDataSource(getJdbcTemplate().getDataSource());
        itemReader.setSql("select ID, NAME, VALUE from T_FOOS where ID > ? and ID < ?");
        itemReader.setIgnoreWarnings(true);
        itemReader.setVerifyCursorPosition(true);

        itemReader.setMapper(new FooRowMapper());
        itemReader.setFetchSize(10);
        itemReader.setMaxRows(100);
        itemReader.setQueryTimeout(1000);
        itemReader.setSaveState(true);
        StepExecutionPreparedStatementSetter pss = new StepExecutionPreparedStatementSetter();
        JobParameters jobParameters = new JobParametersBuilder().addLong("begin.id", new Long(1)).addLong("end.id", new Long(4)).toJobParameters();
        JobInstance jobInstance = new JobInstance(new Long(1), jobParameters, "simpleJob");
        JobExecution jobExecution = new JobExecution(jobInstance, new Long(2));
        StepExecution stepExecution = new StepExecution("taskletStep", jobExecution, new Long(3));
        pss.beforeStep(stepExecution);

        List parameterNames = new ArrayList();
        parameterNames.add("begin.id");
        parameterNames.add("end.id");
        pss.setParameterKeys(parameterNames);

        itemReader.setPreparedStatementSetter(pss);
    }

    @org.junit.Test
public void testRead() throws Exception {
        itemReader.open(new ExecutionContext());
        Foo foo = (Foo) itemReader.read();
        assertEquals(2, foo.getId());
        foo = (Foo) itemReader.read();
        assertEquals(3, foo.getId());
        assertNull(itemReader.read());
    }
	
	/*protected String[] getConfigLocations() {
		return new String[] { ClassUtils.addResourcePathToPackagePath(AbstractJobDaoTests.class, "data-source-context.xml") };
	}*/
}
