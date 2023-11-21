package org.springframework.batch.core.repository.dao;

import org.junit.Before;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.repeat.ExitStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ClassUtils;

import static org.junit.Assert.*;
import static org.junit.Assert.*;

@ContextConfiguration({"sql-dao-test.xml"})
@Transactional
public class JdbcStepExecutionDaoTests extends AbstractStepExecutionDaoTests {

    @Autowired
    StepExecutionDao stepExecutionDao;
    @Autowired
    JobRepository jobRepository;

    @Before
    public void init() throws Exception {
        deleteFromTables(new String[]{"BATCH_EXECUTION_CONTEXT", "BATCH_STEP_EXECUTION", "BATCH_JOB_EXECUTION",
                "BATCH_JOB_PARAMS", "BATCH_JOB_INSTANCE"});
        super.init();
    }

    protected StepExecutionDao getStepExecutionDao() {
        return stepExecutionDao;
    }

    protected JobRepository getJobRepository() {
        return jobRepository;
    }

	/*protected String[] getConfigLocations() {
		return new String[] { ClassUtils.addResourcePathToPackagePath(getClass(), "sql-dao-test.xml") };
	}*/

    /**
     * Long exit descriptions are truncated on both save and update.
     */
    @org.junit.Test
    public void testTruncateExitDescription() {

        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < 100; i++) {
            sb.append("too long exit description");
        }
        String longDescription = sb.toString();

        ExitStatus exitStatus = ExitStatus.FAILED.addExitDescription(longDescription);

        stepExecution.setExitStatus(exitStatus);

        ((JdbcStepExecutionDao) dao).setExitMessageLength(250);
        dao.saveStepExecution(stepExecution);

        StepExecution retrievedAfterSave = dao.getStepExecution(jobExecution, step);

        assertTrue("Exit description should be truncated", retrievedAfterSave.getExitStatus().getExitDescription()
                .length() < stepExecution.getExitStatus().getExitDescription().length());

        dao.updateStepExecution(stepExecution);

        StepExecution retrievedAfterUpdate = dao.getStepExecution(jobExecution, step);

        assertTrue("Exit description should be truncated", retrievedAfterUpdate.getExitStatus().getExitDescription()
                .length() < stepExecution.getExitStatus().getExitDescription().length());
    }
}
