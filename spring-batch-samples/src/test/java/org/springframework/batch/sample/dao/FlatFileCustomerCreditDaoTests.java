package org.springframework.batch.sample.dao;

import java.math.BigDecimal;

import static org.junit.Assert.*;

import org.easymock.MockControl;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.sample.domain.CustomerCredit;

import static org.junit.Assert.*;

public class FlatFileCustomerCreditDaoTests {

    private MockControl outputControl;
    private ResourceLifecycleItemWriter output;
    private FlatFileCustomerCreditDao writer;

    @org.junit.Before
    public void setUp() throws Exception {


        //create mock for OutputSource
        outputControl = MockControl.createControl(ResourceLifecycleItemWriter.class);
        output = (ResourceLifecycleItemWriter) outputControl.getMock();

        //create new writer
        writer = new FlatFileCustomerCreditDao();
        writer.setItemWriter(output);
    }

    @org.junit.Test
    public void testOpen() throws Exception {
        ExecutionContext executionContext = new ExecutionContext();
        //set-up outputSource mock
        output.open(executionContext);
        outputControl.replay();

        //call tested method
        writer.open(executionContext);

        //verify method calls
        outputControl.verify();
    }

    @org.junit.Test
    public void testClose() throws Exception {

        //set-up outputSource mock
        output.close(null);
        outputControl.replay();

        //call tested method
        writer.close();

        //verify method calls
        outputControl.verify();
    }

    @org.junit.Test
    public void testWrite() throws Exception {

        //Create and set-up CustomerCredit
        CustomerCredit credit = new CustomerCredit();
        credit.setCredit(new BigDecimal(1));
        credit.setName("testName");

        //set separator
        writer.setSeparator(";");

        //set-up OutputSource mock
        output.write("testName;1");
        output.open(new ExecutionContext());
        outputControl.replay();

        //call tested method
        writer.writeCredit(credit);

        //verify method calls
        outputControl.verify();
    }

    private interface ResourceLifecycleItemWriter extends ItemWriter, ItemStream {

    }
}
