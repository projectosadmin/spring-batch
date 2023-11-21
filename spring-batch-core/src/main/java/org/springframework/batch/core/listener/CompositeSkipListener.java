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
package org.springframework.batch.core.listener;

import java.util.Iterator;

import org.springframework.batch.core.SkipListener;
import org.springframework.core.Ordered;

/**
 * @author Dave Syer
 * 
 */
public class CompositeSkipListener implements SkipListener {

	private OrderedComposite listeners = new OrderedComposite();

	/**
	 * Public setter for the listeners.
	 * 
	 * @param listeners listeners
	 */
	public void setListeners(SkipListener[] listeners) {
		this.listeners.setItems(listeners);
	}

	/**
	 * Register additional listener.
	 * 
	 * @param listener listener
	 */
	public void register(SkipListener listener) {
		listeners.add(listener);
	}

	/**
	 * Call the registered listeners in order, respecting and prioritising those
	 * that implement {@link Ordered}.
	 * @see org.springframework.batch.core.SkipListener#onSkipInRead(java.lang.Throwable)
	 */
	public void onSkipInRead(Throwable t) {
		for (Iterator iterator = listeners.iterator(); iterator.hasNext();) {
			SkipListener listener = (SkipListener) iterator.next();
			listener.onSkipInRead(t);
		}
	}

	/**
	 * Call the registered listeners in order, respecting and prioritising those
	 * that implement {@link Ordered}.
	 * @see org.springframework.batch.core.SkipListener#onSkipInWrite(java.lang.Object,
	 * java.lang.Throwable)
	 */
	public void onSkipInWrite(Object item, Throwable t) {
		for (Iterator iterator = listeners.iterator(); iterator.hasNext();) {
			SkipListener listener = (SkipListener) iterator.next();
			listener.onSkipInWrite(item, t);
		}
	}
}
