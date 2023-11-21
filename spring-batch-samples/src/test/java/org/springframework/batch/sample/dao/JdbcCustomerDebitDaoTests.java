package org.springframework.batch.sample.dao;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.batch.sample.AbstractDaoTest;
import org.springframework.batch.sample.domain.CustomerDebit;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.test.context.ContextConfiguration;

import static org.junit.Assert.*;

@ContextConfiguration("data-source-context.xml")
public class JdbcCustomerDebitDaoTests extends AbstractDaoTest {

    protected String[] getConfigLocations() {
        return new String[]{"data-source-context.xml"};
    }

    @org.junit.Test
    public void testWrite() {

        //insert customer credit
        jdbcTemplate.execute("INSERT INTO customer VALUES (99, 0, 'testName', 100)");

        //create writer and set jdbcTemplate
        JdbcCustomerDebitDao writer = new JdbcCustomerDebitDao();
        writer.setJdbcTemplate(jdbcTemplate);

        //create customer debit
        CustomerDebit customerDebit = new CustomerDebit();
        customerDebit.setName("testName");
        customerDebit.setDebit(BigDecimal.valueOf(5));

        //call writer
        writer.write(customerDebit);

        //verify customer credit
        jdbcTemplate.query("SELECT name, credit FROM customer WHERE name = 'testName'", new RowCallbackHandler() {
            public void processRow(ResultSet rs) throws SQLException {
                assertEquals(95, rs.getLong("credit"));
            }
        });

    }
}
