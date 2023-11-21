package org.springframework.batch.sample.mapping;

import static org.junit.Assert.*;

import org.springframework.batch.item.file.mapping.FieldSet;
import org.springframework.batch.item.file.mapping.FieldSetMapper;

/**
 * Encapsulates basic logic for testing custom {@link FieldSetMapper} implementations.
 * 
 * @author Robert Kasanicky
 */
public abstract class AbstractFieldSetMapperTests {

	/**
	 * @return <code>FieldSet</code> used for mapping
	 */
	protected abstract FieldSet fieldSet();
	
	/**
	 * @return domain object excepted as a result of mapping the <code>FieldSet</code>
	 * returned by <code>this.fieldSet()</code>
	 */
	protected abstract Object expectedDomainObject();
	
	/**
	 * @return mapper which takes <code>this.fieldSet()</code> and maps it to
	 * domain object.
	 */
	protected abstract FieldSetMapper fieldSetMapper();
	
	
	/**
	 * Regular usage scenario.
	 * Assumes the domain object implements sensible <code>equals(Object other)</code>
	 */
	@org.junit.Test
public void testRegularUse() {
		assertEquals(expectedDomainObject(), fieldSetMapper().mapLine(fieldSet()));
	}
	
}
