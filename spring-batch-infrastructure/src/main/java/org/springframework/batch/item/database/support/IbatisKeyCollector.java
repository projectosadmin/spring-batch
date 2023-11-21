package org.springframework.batch.item.database.support;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.DrivingQueryItemReader;
import org.springframework.batch.item.database.KeyCollector;
import org.springframework.batch.item.util.ExecutionContextUserSupport;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.List;


/**
 * {@link KeyCollector} based on iBATIS ORM framework. It is functionally
 * similar to {@link SingleColumnJdbcKeyCollector} but does not make assumptions
 * about the primary key structure. A separate restart query is necessary to
 * ensure that only the required keys remaining for processing are returned,
 * rather than the entire original list.</p>
 * <p>
 * The writer is thread safe after its properties are set (normal singleton
 * behaviour).
 *
 * @author Robert Kasanicky
 * @author Lucas Ward
 * @see DrivingQueryItemReader
 */
public class IbatisKeyCollector extends ExecutionContextUserSupport implements KeyCollector {

    private static final String RESTART_KEY = "key.index";

    private SqlSessionTemplate sqlSessionTemplate;

    private String drivingQuery;

    private String restartQueryId;

    public IbatisKeyCollector() {
        setName(ClassUtils.getShortName(IbatisKeyCollector.class));
    }

    /*
     * Retrieve the keys using the provided driving query id.
     *
     * @see KeyCollector#retrieveKeys()
     */
    public List retrieveKeys(ExecutionContext executionContext) {
        if (executionContext.containsKey(getKey(RESTART_KEY))) {
            Object key = executionContext.get(getKey(RESTART_KEY));
            return sqlSessionTemplate.selectList(restartQueryId, key);
        } else {
            return sqlSessionTemplate.selectList(drivingQuery);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see KeyCollector#saveState(Object, ExecutionContext)
     */
    public void updateContext(Object key, ExecutionContext executionContext) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(executionContext, "ExecutionContext must be null");
        executionContext.put(getKey(RESTART_KEY), key);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(sqlSessionTemplate, "SqlMaperClientTemplate must not be null.");
        Assert.hasText(drivingQuery, "The DrivingQuery must not be null or empty.");
    }

    /**
     * @param sqlSessionFactory configured iBATIS client
     */
    public void setSqlSessionFactory(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionTemplate = new SqlSessionTemplate(sqlSessionFactory);
    }

    /**
     * @param drivingQueryId id of the iBATIS select statement that will be used
     *                       to retrieve the list of primary keys
     */
    public void setDrivingQueryId(String drivingQueryId) {
        this.drivingQuery = drivingQueryId;
    }

    /**
     * Set the id of the restart query.
     *
     * @param restartQueryId id of the iBatis select statement that will be used
     *                       to retrieve the list of primary keys after a restart.
     */
    public void setRestartQueryId(String restartQueryId) {
        this.restartQueryId = restartQueryId;
    }

    public final SqlSessionTemplate getSqlSessionTemplate() {
        return sqlSessionTemplate;
    }

}
