package org.springframework.batch.item.database;

import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.junit.Before;
import org.springframework.batch.AbstractDaoTest;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.orm.hibernate5.LocalSessionFactoryBean;
import org.springframework.test.context.ContextConfiguration;

import static org.junit.Assert.*;
import static org.junit.Assert.*;
/**
 * Tests for {@link HibernateCursorItemReader} using {@link StatelessSession}.
 * 
 * @author Robert Kasanicky
 */
@ContextConfiguration("/org/springframework/batch/item/database/data-source-context.xml")
public class HibernateCursorProjectionItemReaderIntegrationTests extends AbstractDaoTest {

	protected ItemReader reader;
	protected ExecutionContext executionContext;

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

	
	protected ItemReader createItemReader() throws Exception {
		LocalSessionFactoryBean factoryBean = new LocalSessionFactoryBean();
		factoryBean.setDataSource(super.getJdbcTemplate().getDataSource());
		factoryBean.setMappingLocations(new Resource[] { new ClassPathResource("Foo.hbm.xml", getClass()) });
		factoryBean.afterPropertiesSet();

		SessionFactory sessionFactory = (SessionFactory) factoryBean.getObject();

		String hsqlQuery = "select f.value, f.name from Foo f";

		HibernateCursorItemReader inputSource = new HibernateCursorItemReader();
		inputSource.setQueryString(hsqlQuery);
		inputSource.setSessionFactory(sessionFactory);
		inputSource.afterPropertiesSet();
		inputSource.setSaveState(true);

		return inputSource;
	}

	@org.junit.Test
public void testNormalProcessing() throws Exception {	
		((InitializingBean) reader).afterPropertiesSet();
		((ItemStream) reader).open(new ExecutionContext());
		Object[] foo1 = (Object[]) reader.read();
		assertEquals(new Integer(1), foo1[0]);
	}

}
