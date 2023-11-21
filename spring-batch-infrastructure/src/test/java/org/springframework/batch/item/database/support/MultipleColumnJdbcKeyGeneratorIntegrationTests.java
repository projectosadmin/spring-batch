/**
 * 
 */
package org.springframework.batch.item.database.support;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.springframework.batch.AbstractDaoTest;
import org.springframework.batch.item.ExecutionContext;
import static org.junit.Assert.*;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.ClassUtils;
import static org.junit.Assert.*;
/**
 * @author Lucas Ward
 *
 */
@ContextConfiguration("/org/springframework/batch/item/database/data-source-context.xml")
public class MultipleColumnJdbcKeyGeneratorIntegrationTests extends AbstractDaoTest {
	
	MultipleColumnJdbcKeyCollector keyStrategy;
	
	ExecutionContext executionContext;
	
	protected String[] getConfigLocations(){
		return new String[] { "org/springframework/batch/item/database/data-source-context.xml"};
	}

	@Before
	public void onSetUpBeforeTransaction() throws Exception {

		
		keyStrategy = new MultipleColumnJdbcKeyCollector(getJdbcTemplate(),
		"SELECT ID, VALUE from T_FOOS order by ID");
		
		keyStrategy.setRestartSql("SELECT ID, VALUE from T_FOOS where ID > ? and VALUE > ? order by ID");
		
		executionContext = new ExecutionContext();
	}
	
	@org.junit.Test
public void testRetrieveKeys(){
		
		List keys = keyStrategy.retrieveKeys(executionContext);
		
		for (int i = 0; i < keys.size(); i++) {
			Map id = (Map)keys.get(i);
			assertEquals(id.get("ID"), new Long(i + 1));
			assertEquals(id.get("VALUE"), new Integer(i + 1));
		}
	}
	
	@org.junit.Test
public void testRestoreKeys(){
		
		Map keyMap = new LinkedHashMap();
		keyMap.put("ID", "3");
		keyMap.put("VALUE", "3");
		executionContext.put(ClassUtils.getShortName(MultipleColumnJdbcKeyCollector.class)+ ".current.key", keyMap);
		
		List keys = keyStrategy.retrieveKeys(executionContext);
		
		assertEquals(2, keys.size());
		Map key = (Map)keys.get(0);
		assertEquals(new Long(4), key.get("ID"));
		assertEquals(new Integer(4), key.get("VALUE"));
		key = (Map)keys.get(1);
		assertEquals(new Long(5), key.get("ID"));
		assertEquals(new Integer(5), key.get("VALUE"));
	}
	
//	@org.junit.Test
//public void testGetKeyAsExecutionContext(){
//		
//		Map key = CollectionFactory.createLinkedCaseInsensitiveMapIfPossible(1);
//		key.put("ID", new Long(3));
//		key.put("VALUE", new Integer(4));
//		
//		keyStrategy.setKeyMapper(new KeyMappingPreparedStatementSetter() {
//			public PreparedStatementSetter createSetter(ExecutionContext executionContext) {
//				return null;
//			}
//			public void mapKeys(Object key, ExecutionContext executionContext) {
//				// Just slap the key as a map into the context
//				Map keys = (Map) key;
//				for (Iterator it = keys.entrySet().iterator(); it.hasNext();) {
//					Entry entry = (Entry)it.next();
//					executionContext.put(entry.getKey().toString(), entry.getValue());
//				}
//			}
//			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
//				return null;
//			}
//		});
//		keyStrategy.updateContext(key, executionContext);
//		Properties props = executionContext.getProperties();
//		
//		assertEquals(2, props.size());
//		System.err.println(props);
//		assertEquals("3", props.get("ID"));
//		assertEquals("4", props.get("VALUE"));
//	}
	
	@org.junit.Test
public void testGetNullKeyAsStreamContext(){
		
		try{
			keyStrategy.updateContext(null, null);
			fail();
		}catch(IllegalArgumentException ex){
			//expected
		}
	}
}
