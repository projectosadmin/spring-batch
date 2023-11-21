/**
 * 
 */
package org.springframework.batch.item.database.support;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

import org.easymock.MockControl;
import org.springframework.core.CollectionFactory;
import org.springframework.util.LinkedCaseInsensitiveMap;

import static org.junit.Assert.*;
/**
 * @author Lucas Ward
 */
public class ColumnMapExecutionContextRowMapperTests {

	private ColumnMapItemPreparedStatementSetter mapper;
	
	private Map key;
	
	private MockControl psControl = MockControl.createControl(PreparedStatement.class);
	private PreparedStatement ps;
		
	    @org.junit.Before
public void setUp() throws Exception {
		
	
		ps = (PreparedStatement)psControl.getMock();
		mapper = new ColumnMapItemPreparedStatementSetter();
		
		key = new LinkedCaseInsensitiveMap(2);
		key.put("1", new Integer(1));
		key.put("2", new Integer(2));
	}
	
	@org.junit.Test
public void testSetValuesWithInvalidType() throws Exception {
		
		try{
			mapper.setValues(new Object(), ps);
			fail();
		}catch(IllegalArgumentException ex){
			//expected
		}
	}
	
	@org.junit.Test
public void testCreateExecutionContextWithNull() throws Exception{
		
		try{
			mapper.setValues(ps, null);
			fail();
		}catch(IllegalArgumentException ex){
			//expected
		}
	}
	
	@org.junit.Test
public void testCreateExecutionContextFromEmptyKeys() throws Exception {
		
		psControl.replay();
		mapper.setValues(new HashMap(), ps);
		psControl.verify();
	}
	
	@org.junit.Test
public void testCreateSetter() throws Exception {
		
		ps.setObject(1, new Integer(1));
		ps.setObject(2, new Integer(2));
		psControl.replay();
		mapper.setValues(key, ps);	
		psControl.verify();
	}
	
}
