package org.springframework.batch.item.transform;

import static org.junit.Assert.*;

import org.easymock.MockControl;
import org.springframework.batch.item.ItemWriter;

/**
 * Tests for {@link ItemTransformerItemWriter}.
 * 
 * @author Robert Kasanicky
 */
public class ItemTransformerItemWriterTests {

	private ItemTransformerItemWriter processor = new ItemTransformerItemWriter();

	private ItemTransformer transformer;
	private ItemWriter itemWriter;

	private MockControl tControl = MockControl.createControl(ItemTransformer.class);
	private MockControl outControl = MockControl.createControl(ItemWriter.class);

	    @org.junit.Before
public void setUp() throws Exception {
		transformer = (ItemTransformer) tControl.getMock();
		itemWriter = (ItemWriter) outControl.getMock();
		
		processor.setItemTransformer(transformer);
		processor.setDelegate(itemWriter);
		processor.afterPropertiesSet();
	}

	/**
	 * Regular usage scenario - item is passed to transformer
	 * and the result of transformation is passed to output source.
	 */
	@org.junit.Test
public void testProcess() throws Exception {
		Object item = new Object();
		Object itemAfterTransformation = new Object();

		transformer.transform(item);
		tControl.setReturnValue(itemAfterTransformation);
		
		itemWriter.write(itemAfterTransformation);
		outControl.setVoidCallable();
		
		tControl.replay();
		outControl.replay();

		processor.write(item);

		tControl.verify();
		outControl.verify();
	}
	
	/**
	 * Item transformer must be set.
	 */
	@org.junit.Test
public void testAfterPropertiesSet() throws Exception {
		
		// value not set
		processor.setItemTransformer(null);
		try {
			processor.afterPropertiesSet();
			fail();
		}
		catch (IllegalArgumentException e) {
			// expected
		}
	}
}
