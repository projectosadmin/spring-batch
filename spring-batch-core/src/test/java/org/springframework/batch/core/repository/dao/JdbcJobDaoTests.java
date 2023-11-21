package org.springframework.batch.core.repository.dao;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.batch.core.repository.dao.AbstractJdbcBatchMetadataDao;
import org.springframework.batch.core.repository.dao.JdbcJobExecutionDao;
import org.springframework.batch.core.repository.dao.JdbcJobInstanceDao;
import org.springframework.batch.repeat.ExitStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.Assert.*;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"sql-dao-test.xml"})
@Transactional
public class JdbcJobDaoTests extends AbstractJobDaoTests {

    public static final String LONG_STRING = "A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String ";

/*	@Before
    public void onSetUpBeforeTransaction() throws Exception {
        ((JdbcJobInstanceDao) jobInstanceDao).setTablePrefix(AbstractJdbcBatchMetadataDao.DEFAULT_TABLE_PREFIX);
        ((JdbcJobExecutionDao) jobExecutionDao).setTablePrefix(AbstractJdbcBatchMetadataDao.DEFAULT_TABLE_PREFIX);
    }*/

    @org.junit.Test
    public void testUpdateJobExecutionWithLongExitCode() {

        assertTrue(LONG_STRING.length() > 250);
        ((JdbcJobExecutionDao) jobExecutionDao).setExitMessageLength(250);
        jobExecution.setExitStatus(ExitStatus.FINISHED
                .addExitDescription(LONG_STRING));
        jobExecutionDao.updateJobExecution(jobExecution);

        List executions = jdbcTemplate.queryForList(
                "SELECT * FROM BATCH_JOB_EXECUTION where JOB_INSTANCE_ID=?",
                new Object[]{jobInstance.getId()});
        assertEquals(1, executions.size());
        assertEquals(LONG_STRING.substring(0, 250), ((Map) executions.get(0))
                .get("EXIT_MESSAGE"));
    }

}
