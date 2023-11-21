package org.springframework.batch.io.oxm;

import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

import org.custommonkey.xmlunit.XMLAssert;
import org.custommonkey.xmlunit.XMLUnit;
import org.springframework.batch.io.oxm.domain.Trade;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.item.xml.oxm.MarshallingEventWriterSerializer;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.oxm.Marshaller;
import org.springframework.util.ClassUtils;

public abstract class AbstractStaxEventWriterItemWriterTests {

	private StaxEventItemWriter writer = new StaxEventItemWriter();

	private Resource resource;

	File outputFile;

	protected Resource expected = new ClassPathResource("expected-output.xml", getClass());

	protected List objects = new ArrayList() {
		{
			add(new Trade("isin1", 1, new BigDecimal(1.0), "customer1"));
			add(new Trade("isin2", 2, new BigDecimal(2.0), "customer2"));
			add(new Trade("isin3", 3, new BigDecimal(3.0), "customer3"));
		}
	};

	/**
	 * Write list of domain objects and check the output file.
	 */
	@org.junit.Test
public void testWrite() throws Exception {
		for (Iterator iterator = objects.listIterator(); iterator.hasNext();) {
			writer.write(iterator.next());
		}
		writer.close(null);
		XMLUnit.setIgnoreWhitespace(true);
		XMLAssert.assertXMLEqual(new FileReader(expected.getFile()), new FileReader(resource.getFile()));

	}

	    @org.junit.Before
public void setUp() throws Exception {
		// File outputFile = File.createTempFile("AbstractStaxStreamWriterOutputSourceTests", "xml");
		outputFile = File.createTempFile(ClassUtils.getShortName(this.getClass()), ".xml");
		resource = new FileSystemResource(outputFile);
		writer.setResource(resource);

		MarshallingEventWriterSerializer mapper = new MarshallingEventWriterSerializer(getMarshaller());
		writer.setSerializer(mapper);

		writer.open(new ExecutionContext());
	}

	@org.junit.After
    public void tearDown() throws Exception {
		

		outputFile.delete();
	}

	/**
	 * @return Marshaller specific for the OXM technology being used.
	 */
	protected abstract Marshaller getMarshaller() throws Exception;

}
