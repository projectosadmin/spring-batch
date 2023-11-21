package org.springframework.batch.sample;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.junit.Assert.*;

public class FootballJobFunctionalTests extends
		AbstractValidatingBatchLauncherTests {

	private JdbcOperations jdbcTemplate;

	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	protected void validatePostConditions() throws Exception {
		int count = jdbcTemplate.queryForObject("SELECT COUNT(*) from PLAYER_SUMMARY", Integer.class);
		assertTrue(count>0);
	}

}
