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

package org.springframework.batch.support;

import org.springframework.batch.support.BinaryExceptionClassifier;
import static org.junit.Assert.*;
import static org.junit.Assert.*;

public class BinaryExceptionClassifierTests  {

	BinaryExceptionClassifier classifier = new BinaryExceptionClassifier();

	@org.junit.Test
public void testClassifyNullIsDefault() {
		assertTrue(classifier.isDefault(null));
	}

	@org.junit.Test
public void testClassifyRandomException() {
		assertTrue(classifier.isDefault(new IllegalStateException("foo")));
	}

	@org.junit.Test
public void testClassifyExactMatch() {
		classifier.setExceptionClasses(new Class[] {IllegalStateException.class});
		assertEquals(false, classifier.isDefault(new IllegalStateException("Foo")));
	}

}
