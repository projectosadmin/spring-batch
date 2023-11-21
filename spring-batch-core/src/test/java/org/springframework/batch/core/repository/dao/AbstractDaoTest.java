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

package org.springframework.batch.core.repository.dao;

import org.junit.runner.RunWith;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.JobSupport;
import org.springframework.batch.repeat.ExitStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;


import javax.sql.DataSource;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author Dave Syer
 * 
 */
@Transactional
@RunWith(SpringJUnit4ClassRunner.class)
public abstract class AbstractDaoTest {

	protected JdbcTemplate jdbcTemplate;

	@Autowired
	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	public JdbcTemplate getJdbcTemplate() {
		return this.jdbcTemplate;
	}

	protected void deleteFromTables(String[] names) {
		for (int i = 0; i < names.length; i++) {
			int rowCount = this.jdbcTemplate.update("DELETE FROM " + names[i]);
		}
	}
}
