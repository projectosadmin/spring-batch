package org.springframework.batch.core.repository.dao;

import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration({"sql-dao-test.xml"})
public class MapJobInstanceDaoTests extends AbstractJobInstanceDaoTests {

	protected JobInstanceDao getJobInstanceDao() {
		MapJobInstanceDao.clear();
		return new MapJobInstanceDao();
	}

}
