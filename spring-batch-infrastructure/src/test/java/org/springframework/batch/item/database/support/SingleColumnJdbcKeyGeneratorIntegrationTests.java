package org.springframework.batch.item.database.support;

import java.util.List;

import org.junit.Before;
import org.springframework.batch.AbstractDaoTest;
import org.springframework.batch.item.ExecutionContext;
import static org.junit.Assert.*;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.ClassUtils;
import static org.junit.Assert.*;
/**
 * 
 * @author Lucas Ward
 *
 */
@ContextConfiguration("/org/springframework/batch/item/database/data-source-context.xml")
public class SingleColumnJdbcKeyGeneratorIntegrationTests extends AbstractDaoTest {

	SingleColumnJdbcKeyCollector keyStrategy;
	
	ExecutionContext executionContext;
	
	protected String[] getConfigLocations(){
		return new String[] { "org/springframework/batch/item/database/data-source-context.xml"};
	}

	@Before
	public void onSetUpBeforeTransaction() throws Exception {
		//super.onSetUpBeforeTransaction();
		
		keyStrategy = new SingleColumnJdbcKeyCollector(getJdbcTemplate(),
		"SELECT ID from T_FOOS order by ID");
		
		keyStrategy.setRestartSql("SELECT ID from T_FOOS where ID > ? order by ID");
		
		executionContext = new ExecutionContext();
	}
	
	@org.junit.Test
public void testRetrieveKeys(){
		
		List keys = keyStrategy.retrieveKeys(new ExecutionContext());
		
		for (int i = 0; i < keys.size(); i++) {
			Long id = (Long)keys.get(i);
			assertEquals(new Long(i + 1), id);
		}
		for (int i = 0; i < keys.size(); i++) {
			System.out.println(keys.get(i));
		}
	
	}
	
	@org.junit.Test
public void testRestoreKeys(){
		
		keyStrategy.updateContext(new Long(3), executionContext);
		
		List keys = keyStrategy.retrieveKeys(executionContext);
		
		assertEquals(2, keys.size());
		assertEquals(new Long(4), keys.get(0));
		assertEquals(new Long(5), keys.get(1));
		
		for (int i = 0; i < keys.size(); i++) {
			System.out.println(keys.get(i));
		}
	}
	
	@org.junit.Test
public void testGetKeyAsStreamContext(){
		
		keyStrategy.updateContext(new Long(3), executionContext);
		
		assertEquals(1, executionContext.size());
		assertEquals(new Long(3), executionContext.get(ClassUtils.getShortName(SingleColumnJdbcKeyCollector.class) + ".key"));
	}
	
	@org.junit.Test
public void testGetNullKeyAsStreamContext(){
		
		try{
			keyStrategy.updateContext(null, null);
			fail();
		}catch(IllegalArgumentException ex){
			//expected
		}
	}
	
	@org.junit.Test
public void testRestoreKeysFromNull(){
		
		try{
			keyStrategy.updateContext(null, null);
		}catch(IllegalArgumentException ex){
			//expected
		}
	}
}
