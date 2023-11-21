/*
 * Copyright 2006-2007 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.core.step.item;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.JobSupport;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.AbstractDaoTest;
import org.springframework.batch.core.repository.dao.MapJobExecutionDao;
import org.springframework.batch.core.repository.dao.MapJobInstanceDao;
import org.springframework.batch.core.repository.dao.MapStepExecutionDao;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.AbstractStep;
import org.springframework.batch.item.support.AbstractBufferedItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Lucas Ward
 */
@ContextConfiguration({"/org/springframework/batch/core/repository/dao/sql-dao-test.xml"})
public class SkipIntegrationTests extends AbstractDaoTest {

    private List processed = new ArrayList();
    private int skipCount = 0;

    private SkipLimitStepFactoryBean step;

    private Job job;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private DataSource dataSource;

    private JobRepository jobRepository;

    /**
     * Public setter for the PlatformTransactionManager.
     *
     * @param transactionManager the transactionManager to set
     */
    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    /**
     * Public setter for the DataSource.
     *
     * @param dataSource the dataSource to set
     */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Before
    public void onSetUp() throws Exception {
        MapJobInstanceDao.clear();
        MapStepExecutionDao.clear();
        MapJobExecutionDao.clear();

        JobRepositoryFactoryBean jobRepositoryFactoryBean = new JobRepositoryFactoryBean();
        jobRepositoryFactoryBean.setDatabaseType("hsql");
        jobRepositoryFactoryBean.setDataSource(dataSource);
        jobRepositoryFactoryBean.setTransactionManager(transactionManager);
        jobRepositoryFactoryBean.afterPropertiesSet();
        jobRepository = (JobRepository) jobRepositoryFactoryBean.getObject();

        step = new SkipLimitStepFactoryBean();
        step.setJobRepository(jobRepository);
        step.setTransactionManager(transactionManager);
        step.setBeanName("skipTest");
        step.setCommitInterval(3);
        step.setSkipLimit(10);
        step.setSkippableExceptionClasses(new Class[]{Exception.class});

        job = new JobSupport("FOO");

        step.setTransactionManager(transactionManager);

    }

    /*
     * (non-Javadoc)
     * @see org.springframework.test.AbstractSingleSpringContextTests#getConfigLocations()
     */
/*
	protected String[] getConfigLocations() {
		return new String[] { ClassUtils.addResourcePathToPackagePath(AbstractJobDaoTests.class, "sql-dao-test.xml") };
	}
*/

@org.junit.Test
public void testSkip() throws Exception {

        step.setItemReader(new FooReader());
        step.setItemWriter(new ItemWriterStub());

        JobExecution jobExecution = jobRepository.createJobExecution(job,
                new JobParametersBuilder().addLong("time", new Long(System.currentTimeMillis())).toJobParameters());
        StepExecution stepExecution = new StepExecution(step.getName(), jobExecution);

        AbstractStep abstractStep = (AbstractStep) step.getObject();
        abstractStep.execute(stepExecution);

        Iterator it = processed.iterator();
        assertEquals(0, nextFooId(it));
        assertEquals(1, nextFooId(it));
        assertEquals(3, nextFooId(it));
        assertEquals(4, nextFooId(it));
        assertEquals(5, nextFooId(it));
        assertEquals(7, nextFooId(it));
        assertEquals(8, nextFooId(it));
    }

    private int nextFooId(Iterator it) {
        return ((Foo) it.next()).getId();
    }

@org.junit.Test
public void testSkipListener() throws Exception {

        skipCount = 0;

        SkipListener listener = new SkipListener() {

            public void onSkipInRead(Throwable t) {
                skipCount++;
            }

            public void onSkipInWrite(Object item, Throwable t) {
            }
        };

        step.setListeners(new StepListener[]{listener});

        testSkip();

        assertEquals(3, skipCount);
    }

@org.junit.Test
public void testExceptionInSkipListener() throws Exception {

        skipCount = 0;

        SkipListener listener = new SkipListener() {

            public void onSkipInRead(Throwable t) {
                throw new RuntimeException();
            }

            public void onSkipInWrite(Object item, Throwable t) {
            }
        };

        step.setListeners(new StepListener[]{listener});

        try {
            testSkip();
        } catch (RuntimeException e) {
            //expected
        }
    }

@org.junit.Test
public void testExceptionInItemReadListener() throws Exception {

        skipCount = 0;

        ItemReadListener listener = new ItemReadListener() {

            public void afterRead(Object item) {
            }

            public void beforeRead() {
            }

            public void onReadError(Exception ex) {
                throw new RuntimeException();
            }
        };

        step.setListeners(new StepListener[]{listener});

        testSkip();
    }

    private class FooReader extends AbstractBufferedItemReaderItemStream {

        int fooCounter = 0;

        public FooReader() {
            super.setName("fooReader");
        }

        protected Object doRead() throws Exception {
            if (fooCounter == 10) {
                return null;
            } else if (fooCounter == 2 || fooCounter == 6 || fooCounter == 9) {
                fooCounter++;
                throw new Exception();
            } else {
                return new Foo(fooCounter++);
            }
        }

        protected void doClose() throws Exception {
        }

        protected void doOpen() throws Exception {
        }
    }

    private class Foo {

        final int id;

        public Foo(int id) {
            this.id = id;
        }

        public String toString() {
            return "Foo " + id;
        }

        public int getId() {
            return id;
        }
    }

    private class ItemWriterStub extends AbstractItemWriter {

        public void write(Object item) throws Exception {
            processed.add(item);
        }

    }
}
