package org.springframework.batch.item.adapter;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.sample.FooService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;



import static org.junit.Assert.*;

/**
 * Tests for {@link ItemReaderAdapter}.
 *
 * @author Robert Kasanicky
 */
@ContextConfiguration("delegating-item-provider.xml")
@RunWith(SpringJUnit4ClassRunner.class)
public class ItemReaderAdapterTests {

    @Autowired
    private ItemReaderAdapter provider;
    @Autowired
    private FooService fooService;

    protected String getConfigPath() {
        return "delegating-item-provider.xml";
    }

    /**
     * Regular usage scenario - items are retrieved from the service injected invoker points to.
     */
    @org.junit.Test
    public void testNext() throws Exception {
        List returnedItems = new ArrayList();
        Object item;
        while ((item = provider.read()) != null) {
            returnedItems.add(item);
        }

        List input = fooService.getGeneratedFoos();
        assertEquals(input.size(), returnedItems.size());
        assertFalse(returnedItems.isEmpty());

        for (int i = 0; i < input.size(); i++) {
            assertSame(input.get(i), returnedItems.get(i));
        }
    }

    public void setProvider(ItemReaderAdapter provider) {
        this.provider = provider;
    }

    public void setFooService(FooService fooService) {
        this.fooService = fooService;
    }

}
