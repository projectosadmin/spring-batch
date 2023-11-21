package org.springframework.batch.sample;

import javax.sql.DataSource;

import org.springframework.batch.sample.item.writer.StagingItemWriter;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.junit.Assert.*;

public class ParallelJobFunctionalTests extends
		AbstractValidatingBatchLauncherTests {

	private JdbcOperations jdbcTemplate;

	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	protected void validatePostConditions() throws Exception {
		int count;
		count = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) from BATCH_STAGING where PROCESSED=?",
				Integer.class,
				new Object[] {StagingItemWriter.NEW});
		assertEquals(0, count);
		int total = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) from BATCH_STAGING", Integer.class);
		count = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) from BATCH_STAGING where PROCESSED=?",
				Integer.class,
				new Object[] {StagingItemWriter.DONE});
		assertEquals(total, count);
	}

}
