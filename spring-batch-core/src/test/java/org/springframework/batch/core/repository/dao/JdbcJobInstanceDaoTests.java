package org.springframework.batch.core.repository.dao;

import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.ClassUtils;

import static org.junit.Assert.*;

@ContextConfiguration({"sql-dao-test.xml"})
public class JdbcJobInstanceDaoTests extends AbstractJobInstanceDaoTests {

    @Autowired
    JobInstanceDao jobInstanceDao;

    @Before
    public void init() {
        deleteFromTables(new String[]{"BATCH_EXECUTION_CONTEXT", "BATCH_STEP_EXECUTION", "BATCH_JOB_EXECUTION",
                "BATCH_JOB_PARAMS", "BATCH_JOB_INSTANCE"});
    }

    protected JobInstanceDao getJobInstanceDao() {

        return jobInstanceDao;
    }
	
	/*protected String[] getConfigLocations() {
		return new String[] { ClassUtils.addResourcePathToPackagePath(getClass(), "sql-dao-test.xml") };
	}*/

}
