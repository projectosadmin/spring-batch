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

package org.springframework.batch.repeat.exception;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Before;
import org.springframework.batch.repeat.RepeatContext;
import org.springframework.batch.support.ExceptionClassifierSupport;

import java.io.StringWriter;

import static org.junit.Assert.*;

public class LogOrRethrowExceptionHandlerTests {

    private LogOrRethrowExceptionHandler handler = new LogOrRethrowExceptionHandler();
    private StringWriter writer;
    private RepeatContext context = null;

    @org.junit.Before
public void setUp() throws Exception {
        Logger logger = Logger.getLogger(LogOrRethrowExceptionHandler.class);
        logger.setLevel(Level.DEBUG);
        writer = new StringWriter();
        logger.removeAllAppenders();
        logger.getParent().removeAllAppenders();
        logger.addAppender(new WriterAppender(new SimpleLayout(), writer));
    }

    @org.junit.Test
    public void testRuntimeException() throws Throwable {
        try {
            handler.handleException(context, new RuntimeException("Foo"));
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            assertEquals("Foo", e.getMessage());
        }
    }

    @org.junit.Test
    public void testError() throws Throwable {
        try {
            handler.handleException(context, new Error("Foo"));
            fail("Expected Error");
        } catch (Error e) {
            assertEquals("Foo", e.getMessage());
        }
    }

    @org.junit.Test
    public void testNotRethrownErrorLevel() throws Throwable {
        handler.setExceptionClassifier(new ExceptionClassifierSupport() {
            public Object classify(Throwable throwable) {
                return LogOrRethrowExceptionHandler.ERROR;
            }
        });
        // No exception...
        handler.handleException(context, new Error("Foo"));
        assertNotNull(writer.toString());
    }

    @org.junit.Test
    public void testNotRethrownWarnLevel() throws Throwable {
        handler.setExceptionClassifier(new ExceptionClassifierSupport() {
            public Object classify(Throwable throwable) {
                return LogOrRethrowExceptionHandler.WARN;
            }
        });
        // No exception...
        handler.handleException(context, new Error("Foo"));
        assertNotNull(writer.toString());
    }

    /*    @org.junit.Test
        @Ignore*/
    public void testNotRethrownDebugLevel() throws Throwable {
        handler.setExceptionClassifier(new ExceptionClassifierSupport() {
            public Object classify(Throwable throwable) {
                return LogOrRethrowExceptionHandler.DEBUG;
            }
        });
        // No exception...
        handler.handleException(context, new Error("Foo"));
        assertNotNull(writer.toString());
    }

    @org.junit.Test
    public void testUnclassifiedException() throws Throwable {
        handler.setExceptionClassifier(new ExceptionClassifierSupport() {
            public Object classify(Throwable throwable) {
                return "DEFAULT";
            }
        });
        try {
            handler.handleException(context, new Error("Foo"));
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().toLowerCase().indexOf("unclassified") >= 0);
        }
    }

}
