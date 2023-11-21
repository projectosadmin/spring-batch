package org.springframework.batch.item.file.transform;

import static org.junit.Assert.*;

public class RangeArrayPropertyEditorTests {

	private Range[] ranges;
	private RangeArrayPropertyEditor pe;

	    @org.junit.Before
public void setUp() {

		ranges = null;

		pe = new RangeArrayPropertyEditor() {
			public void setValue(Object value) {
				ranges = (Range[]) value;
			}

			public Object getValue() {
				return ranges;
			}
		};
	}

	@org.junit.Test
public void testSetAsText() {
		pe.setAsText("15, 32, 1-10, 33");

		// result should be 15-31, 32-32, 1-10, 33-unbound
		assertEquals(4, ranges.length);
		assertEquals(15, ranges[0].getMin());
		assertEquals(31, ranges[0].getMax());
		assertEquals(32, ranges[1].getMin());
		assertEquals(32, ranges[1].getMax());
		assertEquals(1, ranges[2].getMin());
		assertEquals(10, ranges[2].getMax());
		assertEquals(33, ranges[3].getMin());
		assertFalse(ranges[3].hasMaxValue());
	}

	@org.junit.Test
public void testSetAsTextWithNoSpaces() {
		pe.setAsText("15,32");

		// result should be 15-31, 32-unbound
		assertEquals(2, ranges.length);
		assertEquals(15, ranges[0].getMin());
		assertEquals(31, ranges[0].getMax());
		assertEquals(32, ranges[1].getMin());
		assertFalse(ranges[1].hasMaxValue());
	}

	@org.junit.Test
public void testGetAsText() {

		ranges = new Range[] { new Range(20), new Range(6, 15), new Range(2),
				new Range(26, 95) };
		assertEquals("20, 6-15, 2, 26-95", pe.getAsText());
	}

	@org.junit.Test
public void testValidDisjointRanges() {
		pe.setForceDisjointRanges(true);

		// test disjoint ranges
		pe.setAsText("1-5,11-15");

		assertEquals(2, ranges.length);
		assertEquals(1, ranges[0].getMin());
		assertEquals(5, ranges[0].getMax());
		assertEquals(11, ranges[1].getMin());
		assertEquals(15, ranges[1].getMax());

	}

	@org.junit.Test
public void testInvalidOverlappingRanges() {

		pe.setForceDisjointRanges(true);

		// test joint ranges
		try {
			pe.setAsText("1-10, 5-15");
			fail("Exception expected: ranges are not disjoint");
		} catch (IllegalArgumentException iae) {
			// expected
		}
	}

	@org.junit.Test
public void testValidOverlappingRanges() {

		// test joint ranges
		pe.setAsText("1-10, 5-15");
		assertEquals(2, ranges.length);
		assertEquals(1, ranges[0].getMin());
		assertEquals(10, ranges[0].getMax());
		assertEquals(5, ranges[1].getMin());
		assertEquals(15, ranges[1].getMax());

	}

	@org.junit.Test
public void testInvalidInput() {

		try {
			pe.setAsText("1-5, b");
			fail("Exception expected: 2nd range is invalid");
		} catch (IllegalArgumentException iae) {
			// expected
		}
	}
}
