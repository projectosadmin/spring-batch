package org.springframework.batch.core.repository.dao;

import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration({"sql-dao-test.xml"})
public class JdbcJobExecutionDaoTests extends AbstractJobExecutionDaoTests {

	@Autowired
	JobExecutionDao dao;

	@Before
	public void onSetUp() {
		deleteFromTables(new String[] { "BATCH_EXECUTION_CONTEXT", "BATCH_STEP_EXECUTION", "BATCH_JOB_EXECUTION",
				"BATCH_JOB_PARAMS", "BATCH_JOB_INSTANCE" });

		// job instance needs to exist before job execution can be created
		getJdbcTemplate()
				.execute(
						"insert into BATCH_JOB_INSTANCE (JOB_INSTANCE_ID, JOB_NAME, JOB_KEY, VERSION) values (1,'execTestJob', '', 0)");

	}

	@Override
	JobExecutionDao getJobExecutionDao() {
		return dao;
	}

/*
	protected String[] getConfigLocations() {
		return new String[] { ClassUtils.addResourcePathToPackagePath(getClass(), "sql-dao-test.xml") };
	}
*/

}
