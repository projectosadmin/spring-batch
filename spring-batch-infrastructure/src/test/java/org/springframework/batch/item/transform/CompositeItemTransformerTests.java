package org.springframework.batch.item.transform;

import java.util.ArrayList;

import static org.junit.Assert.*;

import org.easymock.MockControl;

/**
 * Tests for {@link CompositeItemTransformer}.
 * 
 * @author Robert Kasanicky
 */
public class CompositeItemTransformerTests {

	private CompositeItemTransformer composite = new CompositeItemTransformer();
	
	private ItemTransformer transformer1;
	private ItemTransformer transformer2;

	private MockControl tControl1 = MockControl.createControl(ItemTransformer.class);
	private MockControl tControl2 = MockControl.createControl(ItemTransformer.class);
	
	    @org.junit.Before
public void setUp() throws Exception {
		transformer1 = (ItemTransformer) tControl1.getMock();
		transformer2 = (ItemTransformer) tControl2 .getMock();
		
		composite.setItemTransformers(new ArrayList() {{ 
			add(transformer1); add(transformer2); 
		}});
		
		composite.afterPropertiesSet();
	}
	
	/**
	 * Regular usage scenario - item is passed through the processing chain,
	 * return value of the of the last transformation is returned by the composite.
	 */
	@org.junit.Test
public void testTransform() throws Exception {
		Object item = new Object();
		Object itemAfterFirstTransfromation = new Object();
		Object itemAfterSecondTransformation = new Object();

		transformer1.transform(item);
		tControl1.setReturnValue(itemAfterFirstTransfromation);
		
		transformer2.transform(itemAfterFirstTransfromation);
		tControl2.setReturnValue(itemAfterSecondTransformation);
		
		tControl1.replay();
		tControl2.replay();

		assertSame(itemAfterSecondTransformation, composite.transform(item));

		tControl1.verify();
		tControl2.verify();
	}
	
	/**
	 * The list of transformers must not be null or empty and 
	 * can contain only instances of {@link ItemTransformer}.
	 */
	@org.junit.Test
public void testAfterPropertiesSet() throws Exception {
		
		// value not set
		composite.setItemTransformers(null);
		try {
			composite.afterPropertiesSet();
			fail();
		}
		catch (IllegalArgumentException e) {
			// expected
		}
		
		// empty list
		composite.setItemTransformers(new ArrayList());
		try {
			composite.afterPropertiesSet();
			fail();
		}
		catch (IllegalArgumentException e) {
			// expected
		}
		
		// invalid list member
		composite.setItemTransformers(new ArrayList() {{ add(new Object()); }});
		try {
			composite.afterPropertiesSet();
			fail();
		}
		catch (IllegalArgumentException e) {
			// expected
		}
	}
}
