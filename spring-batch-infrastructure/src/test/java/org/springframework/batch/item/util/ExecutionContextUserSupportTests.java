package org.springframework.batch.item.util;

import org.springframework.batch.item.util.ExecutionContextUserSupport;

import static org.junit.Assert.*;

/**
 * Tests for {@link ExecutionContextUserSupport}.
 */
public class ExecutionContextUserSupportTests {

	ExecutionContextUserSupport tested = new ExecutionContextUserSupport();

	/**
	 * Regular usage scenario - prepends the name (supposed to be unique) to
	 * argument.
	 */
	@org.junit.Test
public void testGetKey() {
		tested.setName("uniqueName");
		assertEquals("uniqueName.key", tested.getKey("key"));
	}

	/**
	 * Exception scenario - name must not be empty.
	 */
	@org.junit.Test
public void testGetKeyWithNoNameSet() {
		tested.setName("");
		try {
			tested.getKey("arbitrary string");
			fail();
		}
		catch (IllegalArgumentException e) {
			// expected
		}
	}
}
