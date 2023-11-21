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

package org.springframework.batch.item.file.transform;

import static org.junit.Assert.*;

import org.springframework.batch.item.file.mapping.DefaultFieldSet;

/**
 * Unit tests for {@link FixedLengthLineAggregator}
 * 
 * @author robert.kasanicky
 * @author peter.zozom
 */
public class FixedLengthLineAggregatorTests {

	// object under test
	private FixedLengthLineAggregator aggregator = new FixedLengthLineAggregator();

	/**
	 * If no ranges are specified, IllegalArgumentException is thrown
	 */
	@org.junit.Test
public void testAggregateNullRecordDescriptor() {
		String[] args = { "does not matter what is here" };

		try {
			aggregator.aggregate(new DefaultFieldSet(args));
			fail("should not work with no ranges specified");
		}
		catch (IllegalArgumentException expected) {
			// expected
		}
	}

	/**
	 * Count of aggregated strings does not match the number of columns 
	 */
	@org.junit.Test
public void testAggregateWrongArgumentCount() {
		String[] string = { "only one test string" };
		aggregator.setColumns(new Range[0]);

		try {
			aggregator.aggregate(new DefaultFieldSet(string));
			fail("Exception expected: count of aggregated strings"
					+ " does not match the number of columns");
		}
		catch (IllegalArgumentException expected) {
			// expected
		}
	}

	/**
	 * Text length exceeds the length of the column.
	 */
	@org.junit.Test
public void testAggregateInvalidInputLength() {
		String[] args = { "Oversize" };
		aggregator.setColumns(new Range[] {new Range(1,args[0].length()-1)});
		try {
			aggregator.aggregate(new DefaultFieldSet(args));
			fail("Invalid text length, exception should have been thrown");
		}
		catch (IllegalArgumentException expected) {
			// expected
		}
	}

	/**
	 * Test aggregation
	 */
	@org.junit.Test
public void testAggregate() {
		String[] args = { "Matchsize", "Smallsize" };
		aggregator.setColumns(new Range[] {new Range(1,9), new Range(10,18)});
		String result = aggregator.aggregate(new DefaultFieldSet(args));		
		assertEquals("MatchsizeSmallsize", result);
	}

	/**
	 * Test aggregation with last range unbound
	 */
	@org.junit.Test
public void testAggregateWithLastRangeUnbound() {
		String[] args = { "Matchsize", "Smallsize" };
		aggregator.setColumns(new Range[] {new Range(1,12), new Range(13)});
		String result = aggregator.aggregate(new DefaultFieldSet(args));		
		assertEquals("Matchsize   Smallsize", result);
	}

	
	/**
	 * Test aggregation with right alignment
	 */
	@org.junit.Test
public void testAggregateFormattedRight() {
		String[] args = { "Matchsize", "Smallsize" };
		aggregator.setAlignment(Alignment.RIGHT);
		aggregator.setColumns(new Range[] {new Range(1,13), new Range(14,23)});
		String result = aggregator.aggregate(new DefaultFieldSet(args));
		assertEquals(23,result.length());
		assertEquals(result, "    Matchsize Smallsize");		
	}

	/**
	 * Test aggregation with center alignment
	 */
	@org.junit.Test
public void testAggregateFormattedCenter() {
		String[] args = { "Matchsize", "Smallsize" };
		aggregator.setAlignment(Alignment.CENTER);
		aggregator.setColumns(new Range[] {new Range(1,13), new Range(14,25)});
		String result = aggregator.aggregate(new DefaultFieldSet(args));
		assertEquals(result, "  Matchsize   Smallsize  ");
	}

	/**
	 * Test aggregation with left alignment
	 */
	@org.junit.Test
public void testAggregateWithCustomPadding() {
		String[] args = { "Matchsize", "Smallsize" };
		aggregator.setPadding('.');
		aggregator.setAlignment(Alignment.LEFT);
		aggregator.setColumns(new Range[] {new Range(1,13), new Range(14,24)});
		String result = aggregator.aggregate(new DefaultFieldSet(args));
		assertEquals(result, "Matchsize....Smallsize..");
	}

	/**
	 * Test aggregation with left alignment
	 */
	@org.junit.Test
public void testAggregateFormattedLeft() {
		String[] args = { "Matchsize", "Smallsize" };
		aggregator.setAlignment(Alignment.LEFT);
		aggregator.setColumns(new Range[] {new Range(1,13), new Range(14,24)});
		String result = aggregator.aggregate(new DefaultFieldSet(args));
		assertEquals(result, "Matchsize    Smallsize  ");
	}
	
	/**
	 * If one of the passed arguments is null, string filled with spaces should
	 * be returned
	 */
	@org.junit.Test
public void testAggregateNullArgument() {
		String[] args = { null };
		aggregator.setColumns(new Range[] {new Range(1,3)});
		assertEquals("   ", aggregator.aggregate(new DefaultFieldSet(args)));
	}
}
