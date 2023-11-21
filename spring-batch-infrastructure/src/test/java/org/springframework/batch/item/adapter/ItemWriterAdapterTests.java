package org.springframework.batch.item.adapter;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.sample.Foo;
import org.springframework.batch.item.sample.FooService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;



import static org.junit.Assert.*;

/**
 * Tests for {@link ItemWriterAdapter}.
 *
 * @author Robert Kasanicky
 */
@ContextConfiguration("delegating-item-writer.xml")
@RunWith(SpringJUnit4ClassRunner.class)
public class ItemWriterAdapterTests {

    @Autowired
    private ItemWriter processor;
    @Autowired
    private FooService fooService;

    protected String getConfigPath() {
        return "delegating-item-writer.xml";
    }

    /**
     * Regular usage scenario - input object should be passed to the service the injected invoker points to.
     */
    @org.junit.Test
    public void testProcess() throws Exception {
        Foo foo;
        while ((foo = fooService.generateFoo()) != null) {
            processor.write(foo);
        }

        List input = fooService.getGeneratedFoos();
        List processed = fooService.getProcessedFoos();
        assertEquals(input.size(), processed.size());
        assertFalse(fooService.getProcessedFoos().isEmpty());

        for (int i = 0; i < input.size(); i++) {
            assertSame(input.get(i), processed.get(i));
        }

    }

    // setter for auto-injection
    public void setProcessor(ItemWriter processor) {
        this.processor = processor;
    }

    // setter for auto-injection
    public void setFooService(FooService fooService) {
        this.fooService = fooService;
    }
}
