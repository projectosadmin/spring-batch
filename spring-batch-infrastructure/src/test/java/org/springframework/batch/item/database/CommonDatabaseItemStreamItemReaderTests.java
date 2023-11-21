package org.springframework.batch.item.database;

import javax.sql.DataSource;
import javax.swing.*;

import org.junit.runner.RunWith;
import org.springframework.batch.item.CommonItemStreamItemReaderTests;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

@ContextConfiguration("/org/springframework/batch/item/database/data-source-context.xml")
@RunWith(SpringJUnit4ClassRunner.class)
@Transactional
public abstract class CommonDatabaseItemStreamItemReaderTests extends CommonItemStreamItemReaderTests {

    @Autowired
    private DataSource dataSource;

    protected DataSource getDataSource() {
        return dataSource;
    }

}
