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
package org.springframework.batch.item.database.support;

import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.KeyCollector;
import org.springframework.batch.item.util.ExecutionContextUserSupport;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * <p>
 * Jdbc {@link KeyCollector} implementation that only works for a single column key. A sql query must be passed in which
 * will be used to return a list of keys. Each key will be mapped by a {@link RowMapper} that returns a mapped key. By
 * default, the {@link SingleColumnRowMapper} is used, and will convert keys into well known types at runtime. It is
 * extremely important to note that only one column should be mapped to an object and returned as a key. If multiple
 * columns are returned as a key in this strategy, then restart will not function properly. Instead a strategy that
 * supports keys comprised of multiple columns should be used.
 * </p>
 * 
 * <p>
 * Restartability: Because the key is only one column, restart is made much more simple. Before each commit, the last
 * processed key is returned to be stored as restart data. Upon restart, that same key is given back to restore from,
 * using a separate 'RestartQuery'. This means that only the keys remaining to be processed are returned, rather than
 * returning the original list of keys and iterating forward to that last committed point.
 * </p>
 * 
 * @author Lucas Ward
 * @see SingleColumnRowMapper
 */
public class SingleColumnJdbcKeyCollector extends ExecutionContextUserSupport implements KeyCollector {

	private static final String RESTART_KEY = "key";

	private JdbcTemplate jdbcTemplate;

	private String sql;

	private String restartSql;

	private RowMapper keyMapper = new SingleColumnRowMapper();

	public SingleColumnJdbcKeyCollector() {
		setName(ClassUtils.getShortName(SingleColumnJdbcKeyCollector.class));
	}

	/**
	 * Constructs a new instance using the provided jdbcTemplate and string representing the sql statement that should
	 * be used to retrieve keys.
	 * 
	 * @param jdbcTemplate jdbcTemplate
	 * @param sql sql
	 * @throws IllegalArgumentException if jdbcTemplate is null.
	 * @throws IllegalArgumentException if sql string is empty or null.
	 */
	public SingleColumnJdbcKeyCollector(JdbcTemplate jdbcTemplate, String sql) {
		this();
		Assert.notNull(jdbcTemplate, "JdbcTemplate must not be null.");
		Assert.hasText(sql, "The sql statement must not be null or empty.");
		this.jdbcTemplate = jdbcTemplate;
		this.sql = sql;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.batch.io.driving.KeyGenerationStrategy#retrieveKeys()
	 */
	public List retrieveKeys(ExecutionContext executionContext) {

		Assert.notNull(executionContext, "The ExecutionContext must not be null");

		if (executionContext.containsKey(getKey(RESTART_KEY))) {
			Assert.state(StringUtils.hasText(restartSql), "The restart sql query must not be null or empty"
			        + " in order to restart.");
			return jdbcTemplate.query(restartSql, new Object[] { executionContext.get(getKey(RESTART_KEY)) }, keyMapper);
		} else {
			return jdbcTemplate.query(sql, keyMapper);
		}
	}

	/**
	 * Get the restart data representing the last processed key.
	 * 
	 * @throws IllegalArgumentException if key is null.
	 */
	public void updateContext(Object key, ExecutionContext executionContext) {
		Assert.notNull(key, "The key must not be null.");
		Assert.notNull(executionContext, "The ExecutionContext must not be null");
		executionContext.put(getKey(RESTART_KEY), key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(jdbcTemplate, "JdbcTemplate must not be null.");
		Assert.hasText(sql, "The DrivingQuery must not be null or empty.");
	}

	/**
	 * Set the {@link RowMapper} to be used to map each key to an object.
	 * 
	 * @param keyMapper keyMapper
	 */
	public void setKeyMapper(RowMapper keyMapper) {
		this.keyMapper = keyMapper;
	}

	/**
	 * Set the SQL query to be used to return the remaining keys to be processed.
	 * 
	 * @param restartSql restartSql
	 */
	public void setRestartSql(String restartSql) {
		this.restartSql = restartSql;
	}

	/**
	 * Set the SQL statement to be used to return the keys to be processed.
	 * 
	 * @param sql sql
	 */
	public void setSql(String sql) {
		this.sql = sql;
	}
	
	/**
	 * Set the {@link JdbcTemplate} to be used.
	 * 
	 * @param jdbcTemplate jdbcTemplate
	 */
	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
}
