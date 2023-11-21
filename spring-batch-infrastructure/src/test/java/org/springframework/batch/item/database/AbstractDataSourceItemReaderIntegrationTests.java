package org.springframework.batch.item.database;

import org.junit.After;
import org.junit.Before;
import org.springframework.batch.AbstractDaoTest;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.sample.Foo;
import org.springframework.beans.factory.InitializingBean;
import static org.junit.Assert.*;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.Assert;
import static org.junit.Assert.*;
/**
 * Common scenarios for testing {@link ItemReader} implementations which read
 * data from database.
 * 
 * @author Lucas Ward
 * @author Robert Kasanicky
 */
@ContextConfiguration("/org/springframework/batch/item/database/data-source-context.xml")
public abstract class AbstractDataSourceItemReaderIntegrationTests extends AbstractDaoTest {

	protected ItemReader reader;
	protected ExecutionContext executionContext;

	/**
	 * @return configured input source ready for use
	 */
	protected abstract ItemReader createItemReader() throws Exception;

	protected String[] getConfigLocations() {
		return new String[] { "org/springframework/batch/item/database/data-source-context.xml" };
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.test.AbstractTransactionalSpringContextTests#onSetUpInTransaction()
	 */
	@Before
	public void onSetUpInTransaction() throws Exception {

		reader = createItemReader();
		executionContext = new ExecutionContext();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.test.AbstractTransactionalSpringContextTests#onTearDownAfterTransaction()
	 */
	@After
	public void onTearDownAfterTransaction() throws Exception {
		getAsItemStream(reader).close(null);

	}

	/**
	 * Regular scenario - read all rows and eventually return null.
	 */
	@org.junit.Test
public void testNormalProcessing() throws Exception {
		getAsInitializingBean(reader).afterPropertiesSet();
		getAsItemStream(reader).open(executionContext);
		
		Foo foo1 = (Foo) reader.read();
		assertEquals(1, foo1.getValue());

		Foo foo2 = (Foo) reader.read();
		assertEquals(2, foo2.getValue());

		Foo foo3 = (Foo) reader.read();
		assertEquals(3, foo3.getValue());

		Foo foo4 = (Foo) reader.read();
		assertEquals(4, foo4.getValue());

		Foo foo5 = (Foo) reader.read();
		assertEquals(5, foo5.getValue());

		assertNull(reader.read());
	}

	/**
	 * Restart scenario - read records, save restart data, create new input
	 * source and restore from restart data - the new input source should
	 * continue where the old one finished.
	 */
	@org.junit.Test
public void testRestart() throws Exception {

		getAsItemStream(reader).open(executionContext);
		
		Foo foo1 = (Foo) reader.read();
		assertEquals(1, foo1.getValue());

		Foo foo2 = (Foo) reader.read();
		assertEquals(2, foo2.getValue());

		getAsItemStream(reader).update(executionContext);

		// create new input source
		reader = createItemReader();

		getAsItemStream(reader).open(executionContext);

		Foo fooAfterRestart = (Foo) reader.read();
		assertEquals(3, fooAfterRestart.getValue());
	}

	/**
	 * Reading from an input source and then trying to restore causes an error.
	 */
	@org.junit.Test
public void testInvalidRestore() throws Exception {

		getAsItemStream(reader).open(executionContext);
		
		Foo foo1 = (Foo) reader.read();
		assertEquals(1, foo1.getValue());

		Foo foo2 = (Foo) reader.read();
		assertEquals(2, foo2.getValue());

		getAsItemStream(reader).update(executionContext);

		// create new input source
		reader = createItemReader();
		getAsItemStream(reader).open(new ExecutionContext());

		Foo foo = (Foo) reader.read();
		assertEquals(1, foo.getValue());

		try {
			getAsItemStream(reader).open(executionContext);
			fail();
		}
		catch (Exception ex) {
			// expected
		}
	}

	/**
	 * Empty restart data should be handled gracefully.
	 * @throws Exception Exception
	 */
	@org.junit.Test
public void testRestoreFromEmptyData() throws Exception {
		getAsItemStream(reader).open(executionContext);

		Foo foo = (Foo) reader.read();
		assertEquals(1, foo.getValue());
	}

	/**
	 * Rollback scenario - input source rollbacks to last commit point.
	 * @throws Exception Exception
	 */
	@org.junit.Test
public void testRollback() throws Exception {
		getAsItemStream(reader).open(executionContext);
		
		Foo foo1 = (Foo) reader.read();

		commit();

		Foo foo2 = (Foo) reader.read();
		Assert.state(!foo2.equals(foo1));

		Foo foo3 = (Foo) reader.read();
		Assert.state(!foo2.equals(foo3));

		rollback();

		assertEquals(foo2, reader.read());
	}

	/**
	 * Rollback scenario with restart - input source rollbacks to last
	 * commit point.
	 * @throws Exception Exception
	 */
	@org.junit.Test
public void testRollbackAndRestart() throws Exception {

		getAsItemStream(reader).open(executionContext);
		
		Foo foo1 = (Foo) reader.read();

		commit();

		Foo foo2 = (Foo) reader.read();
		Assert.state(!foo2.equals(foo1));

		Foo foo3 = (Foo) reader.read();
		Assert.state(!foo2.equals(foo3));

		rollback();

		getAsItemStream(reader).update(executionContext);

		// create new input source
		reader = createItemReader();

		getAsItemStream(reader).open(executionContext);

		assertEquals(foo2, reader.read());
		assertEquals(foo3, reader.read());
	}
	
	/**
	 * Rollback scenario with restart - input source rollbacks to last
	 * commit point.
	 * @throws Exception Exception
	 */
	@org.junit.Test
public void testRollbackOnFirstChunkAndRestart() throws Exception {

		getAsItemStream(reader).open(executionContext);
		
		Foo foo1 = (Foo) reader.read();

		Foo foo2 = (Foo) reader.read();
		Assert.state(!foo2.equals(foo1));

		Foo foo3 = (Foo) reader.read();
		Assert.state(!foo2.equals(foo3));

		rollback();

		getAsItemStream(reader).update(executionContext);

		// create new input source
		reader = createItemReader();

		getAsItemStream(reader).open(executionContext);

		assertEquals(foo1, reader.read());
		assertEquals(foo2, reader.read());
	}
	
	@org.junit.Test
public void testMultipleRestarts() throws Exception {
		
		getAsItemStream(reader).open(executionContext);
		
		Foo foo1 = (Foo) reader.read();

		commit();

		Foo foo2 = (Foo) reader.read();
		Assert.state(!foo2.equals(foo1));

		Foo foo3 = (Foo) reader.read();
		Assert.state(!foo2.equals(foo3));

		rollback();

		getAsItemStream(reader).update(executionContext);

		// create new input source
		reader = createItemReader();

		getAsItemStream(reader).open(executionContext);

		assertEquals(foo2, reader.read());
		assertEquals(foo3, reader.read());
		
		getAsItemStream(reader).update(executionContext);
		
		commit();
		
		// create new input source
		reader = createItemReader();

		getAsItemStream(reader).open(executionContext);
		
		Foo foo4 = (Foo)reader.read();
		Foo foo5 = (Foo)reader.read();
		assertEquals(4, foo4.getValue());
		assertEquals(5, foo5.getValue());
	}

	private void commit() {
		reader.mark();
	}

	private void rollback() {
		reader.reset();
	}

	private ItemStream getAsItemStream(ItemReader source) {
		return (ItemStream) source;
	}

	private InitializingBean getAsInitializingBean(ItemReader source) {
		return (InitializingBean) source;
	}

}
