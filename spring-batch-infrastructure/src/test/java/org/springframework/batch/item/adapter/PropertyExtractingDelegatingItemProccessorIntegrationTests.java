package org.springframework.batch.item.adapter;

import java.util.List;

import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import org.springframework.batch.item.sample.Foo;
import org.springframework.batch.item.sample.FooService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

/**
 * Tests for {@link PropertyExtractingDelegatingItemWriter}
 *
 * @author Robert Kasanicky
 */
@ContextConfiguration("pe-delegating-item-writer.xml")
@RunWith(SpringJUnit4ClassRunner.class)
public class PropertyExtractingDelegatingItemProccessorIntegrationTests {

    @Autowired
    private PropertyExtractingDelegatingItemWriter processor;

    @Autowired
    private FooService fooService;

    protected String getConfigPath() {
        return "pe-delegating-item-writer.xml";
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
        List processed = fooService.getProcessedFooNameValuePairs();
        assertEquals(input.size(), processed.size());
        assertFalse(fooService.getProcessedFooNameValuePairs().isEmpty());

        for (int i = 0; i < input.size(); i++) {
            Foo inputFoo = (Foo) input.get(i);
            Foo outputFoo = (Foo) processed.get(i);
            assertEquals(inputFoo.getName(), outputFoo.getName());
            assertEquals(inputFoo.getValue(), outputFoo.getValue());
            assertEquals(0, outputFoo.getId());
        }

    }

    public void setProcessor(PropertyExtractingDelegatingItemWriter processor) {
        this.processor = processor;
    }

    public void setFooService(FooService fooService) {
        this.fooService = fooService;
    }

}
