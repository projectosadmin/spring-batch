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
package org.springframework.batch.sample.advice;

import java.util.ArrayList;
import java.util.List;

import javax.management.Notification;

import static org.junit.Assert.*;

import org.springframework.jmx.export.notification.NotificationPublisher;
import org.springframework.jmx.export.notification.UnableToSendNotificationException;

import static org.junit.Assert.*;

/**
 * @author Dave Syer
 */
public class JobExecutionNotificationPublisherTests {

    JobExecutionNotificationPublisher publisher = new JobExecutionNotificationPublisher();

    @org.junit.Test
    public void testRepeatOperationsOpenUsed() throws Exception {
        final List list = new ArrayList();
        publisher.setNotificationPublisher(new NotificationPublisher() {
            public void sendNotification(Notification notification) throws UnableToSendNotificationException {
                list.add(notification);
            }
        });
        publisher.onApplicationEvent(new SimpleMessageApplicationEvent(this, "foo"));
        assertEquals(1, list.size());
        String message = ((Notification) list.get(0)).getMessage();
        assertTrue("Message does not contain 'foo': ", message.indexOf("foo") > 0);
    }

}
