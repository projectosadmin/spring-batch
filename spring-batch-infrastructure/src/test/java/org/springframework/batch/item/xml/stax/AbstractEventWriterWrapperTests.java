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
package org.springframework.batch.item.xml.stax;

import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

import static org.junit.Assert.*;

import org.easymock.MockControl;

import com.bea.xml.stream.events.StartDocumentEvent;
import com.bea.xml.stream.util.NamespaceContextImpl;

/**
 * @author Lucas Ward
 *
 */
public class AbstractEventWriterWrapperTests {

	AbstractEventWriterWrapper eventWriterWrapper;
	MockControl mockEventWriterControl = MockControl.createControl(XMLEventWriter.class);
	XMLEventWriter xmlEventWriter;

	    @org.junit.Before
public void setUp() throws Exception {
		

		xmlEventWriter = (XMLEventWriter)mockEventWriterControl.getMock();
		eventWriterWrapper = new StubEventWriter(xmlEventWriter);
	}

	@org.junit.Test
public void testAdd() throws XMLStreamException {

		XMLEvent event = new StartDocumentEvent();
		xmlEventWriter.add(event);
		mockEventWriterControl.replay();
		eventWriterWrapper.add(event);
		mockEventWriterControl.verify();

	}

	@org.junit.Test
public void testAddReader() throws XMLStreamException {

		MockControl readerControl = MockControl.createControl(XMLEventReader.class);
		XMLEventReader reader = (XMLEventReader)readerControl.getMock();
		xmlEventWriter.add(reader);
		mockEventWriterControl.replay();
		eventWriterWrapper.add(reader);
		mockEventWriterControl.verify();
	}

	@org.junit.Test
public void testClose() throws XMLStreamException {
		xmlEventWriter.close();
		mockEventWriterControl.replay();
		eventWriterWrapper.close();
		mockEventWriterControl.verify();
	}

	@org.junit.Test
public void testFlush() throws XMLStreamException {
		xmlEventWriter.flush();
		mockEventWriterControl.replay();
		eventWriterWrapper.flush();
		mockEventWriterControl.verify();
	}

	@org.junit.Test
public void testGetNamespaceContext() {
		NamespaceContext context = new NamespaceContextImpl();
		xmlEventWriter.getNamespaceContext();
		mockEventWriterControl.setReturnValue(context);
		mockEventWriterControl.replay();
		assertEquals(eventWriterWrapper.getNamespaceContext(), context);
		mockEventWriterControl.verify();
	}

	@org.junit.Test
public void testGetPrefix() throws XMLStreamException {

		String uri = "uri";
		xmlEventWriter.getPrefix(uri);
		mockEventWriterControl.setReturnValue(uri);
		mockEventWriterControl.replay();
		assertEquals(eventWriterWrapper.getPrefix(uri), uri);
		mockEventWriterControl.verify();
	}

	@org.junit.Test
public void testSetDefaultNamespace() throws XMLStreamException {
		String uri = "uri";
		xmlEventWriter.setDefaultNamespace(uri);
		mockEventWriterControl.replay();
		eventWriterWrapper.setDefaultNamespace(uri);
		mockEventWriterControl.verify();
	}

	@org.junit.Test
public void testSetNamespaceContext()
			throws XMLStreamException {

		NamespaceContext context = new NamespaceContextImpl();
		xmlEventWriter.setNamespaceContext(context);
		mockEventWriterControl.replay();
		eventWriterWrapper.setNamespaceContext(context);
		mockEventWriterControl.verify();
	}

	@org.junit.Test
public void testSetPrefix() throws XMLStreamException {

		String uri = "uri";
		String prefix = "prefix";
		xmlEventWriter.setPrefix(prefix, uri);
		mockEventWriterControl.replay();
		eventWriterWrapper.setPrefix(prefix, uri);
		mockEventWriterControl.verify();
	}

	private static class StubEventWriter extends AbstractEventWriterWrapper{
		public StubEventWriter(XMLEventWriter wrappedEventWriter) {
			super(wrappedEventWriter);
		}
	}
}
