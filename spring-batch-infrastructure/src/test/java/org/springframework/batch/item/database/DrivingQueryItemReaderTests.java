package org.springframework.batch.item.database;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.sample.Foo;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

@Transactional
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/org/springframework/batch/item/database/data-source-context.xml")
public class DrivingQueryItemReaderTests {

    DrivingQueryItemReader itemReader;

    static {
        TransactionSynchronizationManager.initSynchronization();
    }

    @org.junit.Before
    public void setUp() throws Exception {
        itemReader = createItemReader();
    }

    private DrivingQueryItemReader createItemReader() throws Exception {

        DrivingQueryItemReader inputSource = new DrivingQueryItemReader();
        inputSource.setKeyCollector(new MockKeyGenerator());
        inputSource.setSaveState(true);

        return inputSource;
    }

    /**
     * Regular scenario - read all rows and eventually return null.
     */
    @org.junit.Test
    public void testNormalProcessing() throws Exception {
        getAsInitializingBean(itemReader).afterPropertiesSet();
        getAsItemStream(itemReader).open(new ExecutionContext());

        Foo foo1 = (Foo) itemReader.read();
        assertEquals(1, foo1.getValue());

        Foo foo2 = (Foo) itemReader.read();
        assertEquals(2, foo2.getValue());

        Foo foo3 = (Foo) itemReader.read();
        assertEquals(3, foo3.getValue());

        Foo foo4 = (Foo) itemReader.read();
        assertEquals(4, foo4.getValue());

        Foo foo5 = (Foo) itemReader.read();
        assertEquals(5, foo5.getValue());

        assertNull(itemReader.read());
    }

    /**
     * Restart scenario.
     *
     * @throws Exception Exception
     */
    @org.junit.Test
    public void testRestart() throws Exception {

        ExecutionContext executionContext = new ExecutionContext();

        getAsItemStream(itemReader).open(executionContext);

        Foo foo1 = (Foo) itemReader.read();
        assertEquals(1, foo1.getValue());

        Foo foo2 = (Foo) itemReader.read();
        assertEquals(2, foo2.getValue());

        getAsItemStream(itemReader).update(executionContext);

        // create new input source
        itemReader = createItemReader();

        getAsItemStream(itemReader).open(executionContext);

        Foo fooAfterRestart = (Foo) itemReader.read();
        assertEquals(3, fooAfterRestart.getValue());
    }

    /**
     * Reading from an input source and then trying to restore causes an error.
     */
    @org.junit.Test
    public void testInvalidRestore() throws Exception {

        ExecutionContext executionContext = new ExecutionContext();

        getAsItemStream(itemReader).open(executionContext);

        Foo foo1 = (Foo) itemReader.read();
        assertEquals(1, foo1.getValue());

        Foo foo2 = (Foo) itemReader.read();
        assertEquals(2, foo2.getValue());

        getAsItemStream(itemReader).update(executionContext);

        // create new input source
        itemReader = createItemReader();
        getAsItemStream(itemReader).open(new ExecutionContext());

        Foo foo = (Foo) itemReader.read();
        assertEquals(1, foo.getValue());

        try {
            getAsItemStream(itemReader).open(executionContext);
            fail();
        } catch (IllegalStateException ex) {
            // expected
        }
    }

    /**
     * Empty restart data should be handled gracefully.
     *
     * @throws Exception Exception
     */
    @org.junit.Test
    public void testRestoreFromEmptyData() throws Exception {
        ExecutionContext streamContext = new ExecutionContext(new Properties());

        getAsItemStream(itemReader).open(streamContext);

        Foo foo = (Foo) itemReader.read();
        assertEquals(1, foo.getValue());
    }

    /**
     * Rollback scenario.
     *
     * @throws Exception Exception
     */
    @org.junit.Test
    public void testRollback() throws Exception {
        getAsItemStream(itemReader).open(new ExecutionContext());
        Foo foo1 = (Foo) itemReader.read();

        commit();

        Foo foo2 = (Foo) itemReader.read();
        Assert.state(!foo2.equals(foo1));

        Foo foo3 = (Foo) itemReader.read();
        Assert.state(!foo2.equals(foo3));

        rollback();

        assertEquals(foo2, itemReader.read());
    }

    @org.junit.Test
    public void testRetriveZeroKeys() {

        itemReader.setKeyCollector(new KeyCollector() {

            public List retrieveKeys(ExecutionContext executionContext) {
                return new ArrayList();
            }

            public void updateContext(Object key,
                                      ExecutionContext executionContext) {
            }
        });

        itemReader.open(new ExecutionContext());

        assertNull(itemReader.read());

    }

    private void commit() {
        itemReader.mark();
    }

    private void rollback() {
        itemReader.reset();
    }

    private InitializingBean getAsInitializingBean(ItemReader source) {
        return (InitializingBean) source;
    }

    private ItemStream getAsItemStream(ItemReader source) {
        return (ItemStream) source;
    }

    private static class MockKeyGenerator implements KeyCollector {

        static ExecutionContext streamContext;
        List keys;
        List restartKeys;
        static final String RESTART_KEY = "restart.keys";

        static {
            Properties props = new Properties();
            // restart data properties cannot be empty.
            props.setProperty("", "");

            streamContext = new ExecutionContext(props);
        }

        public MockKeyGenerator() {

            keys = new ArrayList();
            keys.add(new Foo(1, "1", 1));
            keys.add(new Foo(2, "2", 2));
            keys.add(new Foo(3, "3", 3));
            keys.add(new Foo(4, "4", 4));
            keys.add(new Foo(5, "5", 5));

            restartKeys = new ArrayList();
            restartKeys.add(new Foo(3, "3", 3));
            restartKeys.add(new Foo(4, "4", 4));
            restartKeys.add(new Foo(5, "5", 5));
        }

        public ExecutionContext saveState(Object key) {
            return streamContext;
        }

        public List retrieveKeys(ExecutionContext executionContext) {
            if (executionContext.containsKey(RESTART_KEY)) {
                return restartKeys;
            } else {
                return keys;
            }
        }

        public void updateContext(Object key, ExecutionContext executionContext) {
            executionContext.put(RESTART_KEY, restartKeys);
        }

    }

}
