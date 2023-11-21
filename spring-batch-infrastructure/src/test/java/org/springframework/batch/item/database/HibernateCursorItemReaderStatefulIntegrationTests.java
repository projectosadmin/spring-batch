package org.springframework.batch.item.database;

import org.easymock.MockControl;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.batch.item.ExecutionContext;

/**
 * Tests for {@link HibernateCursorItemReader} using standard hibernate {@link Session}.
 * 
 * @author Robert Kasanicky
 */
public class HibernateCursorItemReaderStatefulIntegrationTests extends HibernateCursorItemReaderIntegrationTests {

	protected boolean isUseStatelessSession() {
		return false;
	}
	
	//Ensure close is called on the stateful session correctly.
	@org.junit.Test
public void testStatfulClose(){
		
		MockControl sessionFactoryControl = MockControl.createControl(SessionFactory.class);
		SessionFactory sessionFactory = (SessionFactory) sessionFactoryControl.getMock();
		MockControl sessionControl = MockControl.createControl(Session.class);
		Session session = (Session) sessionControl.getMock();
		MockControl resultsControl = MockControl.createNiceControl(org.hibernate.query.Query.class);
		org.hibernate.query.Query scrollableResults = (org.hibernate.query.Query) resultsControl.getMock();
		HibernateCursorItemReader itemReader = new HibernateCursorItemReader();
		itemReader.setSessionFactory(sessionFactory);
		itemReader.setQueryString("testQuery");
		itemReader.setUseStatelessSession(false);
		
		sessionFactory.openSession();
		sessionFactoryControl.setReturnValue(session);
		session.createQuery("testQuery");
		sessionControl.setReturnValue(scrollableResults);
		scrollableResults.setFetchSize(0);
		resultsControl.setReturnValue(scrollableResults);
		session.close();
		sessionControl.setVoidCallable(1);
		sessionFactoryControl.replay();
		sessionControl.replay();
		resultsControl.replay();
		itemReader.open(new ExecutionContext());
		itemReader.close(new ExecutionContext());
		sessionFactoryControl.verify();
		sessionControl.verify();
	}
	
}
