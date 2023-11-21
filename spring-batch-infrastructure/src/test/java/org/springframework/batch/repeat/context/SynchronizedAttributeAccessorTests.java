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

package org.springframework.batch.repeat.context;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

import org.springframework.core.AttributeAccessorSupport;

public class SynchronizedAttributeAccessorTests {

	SynchronizedAttributeAccessor accessor = new SynchronizedAttributeAccessor();

	@org.junit.Test
public void testHashCode() {
		SynchronizedAttributeAccessor another = new SynchronizedAttributeAccessor();
		accessor.setAttribute("foo", "bar");
		another.setAttribute("foo", "bar");
		assertEquals(accessor, another);
		assertEquals("Object.hashCode() contract broken", accessor.hashCode(), another.hashCode());
	}

	@org.junit.Test
public void testToStringWithNoAttributes() throws Exception {
		assertNotNull(accessor.toString());
	}

	@org.junit.Test
public void testToStringWithAttributes() throws Exception {
		accessor.setAttribute("foo", "bar");
		accessor.setAttribute("spam", "bucket");
		assertNotNull(accessor.toString());
	}

	@org.junit.Test
public void testAttributeNames() {
		accessor.setAttribute("foo", "bar");
		accessor.setAttribute("spam", "bucket");
		List list = Arrays.asList(accessor.attributeNames());
		assertEquals(2, list.size());
		assertTrue(list.contains("foo"));
	}

	@org.junit.Test
public void testEqualsSameType() {
		SynchronizedAttributeAccessor another = new SynchronizedAttributeAccessor();
		accessor.setAttribute("foo", "bar");
		another.setAttribute("foo", "bar");
		assertEquals(accessor, another);
	}

	@org.junit.Test
public void testEqualsSelf() {
		accessor.setAttribute("foo", "bar");
		assertEquals(accessor, accessor);
	}

	@org.junit.Test
public void testEqualsWrongType() {
		accessor.setAttribute("foo", "bar");
		Map another = Collections.singletonMap("foo", "bar");
		// Accessor and another are instances of unrelated classes, they should
		// never be equal...
		assertFalse(accessor.equals(another));
	}

	@org.junit.Test
public void testEqualsSupport() {
		AttributeAccessorSupport another = new AttributeAccessorSupport() {
		};
		accessor.setAttribute("foo", "bar");
		another.setAttribute("foo", "bar");
		assertEquals(accessor, another);
	}

	@org.junit.Test
public void testGetAttribute() {
		accessor.setAttribute("foo", "bar");
		assertEquals("bar", accessor.getAttribute("foo"));
	}

	@org.junit.Test
public void testSetAttributeIfAbsentWhenAlreadyPresent() {
		accessor.setAttribute("foo", "bar");
		assertEquals("bar", accessor.setAttributeIfAbsent("foo", "spam"));
	}

	@org.junit.Test
public void testSetAttributeIfAbsentWhenNotAlreadyPresent() {
		assertEquals(null, accessor.setAttributeIfAbsent("foo", "bar"));
		assertEquals("bar", accessor.getAttribute("foo"));
	}

	@org.junit.Test
public void testHasAttribute() {
		accessor.setAttribute("foo", "bar");
		assertEquals(true, accessor.hasAttribute("foo"));
	}

	@org.junit.Test
public void testRemoveAttribute() {
		accessor.setAttribute("foo", "bar");
		assertEquals("bar", accessor.getAttribute("foo"));
		accessor.removeAttribute("foo");
		assertEquals(null, accessor.getAttribute("foo"));
	}

}
