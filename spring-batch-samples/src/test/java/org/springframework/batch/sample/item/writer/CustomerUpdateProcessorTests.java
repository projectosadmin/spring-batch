package org.springframework.batch.sample.item.writer;

import java.math.BigDecimal;

import static org.junit.Assert.*;

import org.springframework.batch.sample.dao.JdbcCustomerDebitDao;
import org.springframework.batch.sample.domain.CustomerDebit;
import org.springframework.batch.sample.domain.Trade;
import org.springframework.batch.sample.item.writer.CustomerUpdateWriter;

public class CustomerUpdateProcessorTests {

    @org.junit.Test
    public void testProcess() {

        //create trade object
        Trade trade = new Trade();
        trade.setCustomer("testCustomerName");
        trade.setPrice(new BigDecimal(123.0));

        //create dao
        JdbcCustomerDebitDao dao = new JdbcCustomerDebitDao() {
            public void write(CustomerDebit customerDebit) {
                assertEquals("testCustomerName", customerDebit.getName());
                assertEquals(new BigDecimal(123.0), customerDebit.getDebit());
            }
        };

        //create processor and set dao
        CustomerUpdateWriter processor = new CustomerUpdateWriter();
        processor.setDao(dao);

        //call tested method - see asserts in dao.write() method
        processor.write(trade);
    }
}
