/*
 * Copyright 2006-2008 the original author or authors.
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

import javax.sql.DataSource;

import static org.junit.Assert.*;

import org.easymock.MockControl;
import org.junit.Before;
import org.springframework.jdbc.support.incrementer.*;
import static org.junit.Assert.*;
/**
 * @author Lucas Ward
 *
 */
public class DefaultDataFieldMaxValueIncrementerFactoryTests {

	private DefaultDataFieldMaxValueIncrementerFactory factory;
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	@org.junit.Before
public void setUp() throws Exception {
		
		
		DataSource dataSource = (DataSource)MockControl.createControl(DataSource.class).getMock();
		factory = new DefaultDataFieldMaxValueIncrementerFactory(dataSource);
	}
	
	@org.junit.Test
public void testSupportedDatabaseType(){
		assertTrue(factory.isSupportedIncrementerType("db2"));
		assertTrue(factory.isSupportedIncrementerType("db2zos"));
		assertTrue(factory.isSupportedIncrementerType("mysql"));
		assertTrue(factory.isSupportedIncrementerType("derby"));
		assertTrue(factory.isSupportedIncrementerType("oracle"));
		assertTrue(factory.isSupportedIncrementerType("postgres"));
		assertTrue(factory.isSupportedIncrementerType("hsql"));
		assertTrue(factory.isSupportedIncrementerType("sqlserver"));
		assertTrue(factory.isSupportedIncrementerType("sybase"));
	}
	
	@org.junit.Test
public void testUnsupportedDatabaseType(){
		assertFalse(factory.isSupportedIncrementerType("invalidtype"));
	}
	
	@org.junit.Test
public void testInvalidDatabaseType(){
		try{
			factory.getIncrementer("invalidtype", "NAME");
			fail();
		}
		catch(IllegalArgumentException ex){
			//expected
		}
	}
	
	@org.junit.Test
public void testNullIncrementerName(){
		try{
			factory.getIncrementer("db2", null);
			fail();
		}
		catch(IllegalArgumentException ex){
			//expected
		}
	}
	
	@org.junit.Test
public void testDb2(){
		assertTrue(factory.getIncrementer("db2", "NAME") instanceof DB2SequenceMaxValueIncrementer);
	}
	
	@org.junit.Test
public void testDb2zos(){
		assertTrue(factory.getIncrementer("db2zos", "NAME") instanceof DB2MainframeSequenceMaxValueIncrementer);
	}

	@org.junit.Test
public void testMysql(){
		assertTrue(factory.getIncrementer("mysql", "NAME") instanceof MySQLMaxValueIncrementer);
	}

	@org.junit.Test
public void testOracle(){
		factory.setIncrementerColumnName("ID");
		assertTrue(factory.getIncrementer("oracle", "NAME") instanceof OracleSequenceMaxValueIncrementer);
	}

	@org.junit.Test
public void testDerby(){
		assertTrue(factory.getIncrementer("derby", "NAME") instanceof DerbyMaxValueIncrementer);
	}

	@org.junit.Test
public void testHsql(){
		assertTrue(factory.getIncrementer("hsql", "NAME") instanceof HsqlMaxValueIncrementer);
	}
	
	@org.junit.Test
public void testPostgres(){
		assertTrue(factory.getIncrementer("postgres", "NAME") instanceof PostgreSQLSequenceMaxValueIncrementer);
	}

	@org.junit.Test
public void testMsSqlServer(){
		assertTrue(factory.getIncrementer("sqlserver", "NAME") instanceof SqlServerMaxValueIncrementer);
	}

	@org.junit.Test
public void testSybase(){
		assertTrue(factory.getIncrementer("sybase", "NAME") instanceof SybaseMaxValueIncrementer);
	}


}
