/*
 * Copyright 2006-2008 the original author or authors.
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
package org.springframework.batch.core.listener;

import static org.junit.Assert.*;

import org.easymock.MockControl;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.listener.CompositeItemReadListener;

/**
 * @author Lucas Ward
 *
 */
public class CompositeItemReadListenerTests {

	MockControl listenerControl = MockControl.createControl(ItemReadListener.class);
	
	ItemReadListener listener;
	CompositeItemReadListener compositeListener;
	
	    @org.junit.Before
public void setUp() throws Exception {
		
	
		listener = (ItemReadListener)listenerControl.getMock();
		compositeListener = new CompositeItemReadListener();
		compositeListener.register(listener);
	}
	
	@org.junit.Test
public void testBeforeRead(){
		
		listener.beforeRead();
		listenerControl.replay();
		compositeListener.beforeRead();
		listenerControl.verify();
	}
	
	@org.junit.Test
public void testAfterRead(){
		Object item = new Object();
		listener.afterRead(item);
		listenerControl.replay();
		compositeListener.afterRead(item);
		listenerControl.verify();
	}
	
	@org.junit.Test
public void testOnReadError(){
		
		Exception ex = new Exception();
		listener.onReadError(ex);
		listenerControl.replay();
		compositeListener.onReadError(ex);
		listenerControl.verify();
	}

	@org.junit.Test
public void testSetListners() throws Exception {
		compositeListener.setListeners(new ItemReadListener[] {listener});
		listener.beforeRead();
		listenerControl.replay();
		compositeListener.beforeRead();
		listenerControl.verify();
	}
	
}
