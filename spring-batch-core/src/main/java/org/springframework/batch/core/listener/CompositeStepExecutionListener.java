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

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.repeat.ExitStatus;
import org.springframework.core.Ordered;

/**
 * @author Lucas Ward
 * @author Dave Syer
 * 
 */
public class CompositeStepExecutionListener implements StepExecutionListener {

	private OrderedComposite list = new OrderedComposite();

	/**
	 * Public setter for the listeners.
	 * 
	 * @param listeners listeners
	 */
	public void setListeners(StepExecutionListener[] listeners) {
		list.setItems(listeners);
	}

	/**
	 * Register additional listener.
	 * 
	 * @param stepExecutionListener stepExecutionListener
	 */
	public void register(StepExecutionListener stepExecutionListener) {
		list.add(stepExecutionListener);
	}

	/**
	 * Call the registered listeners in reverse order, respecting and
	 * prioritising those that implement {@link Ordered}.
	 * @see org.springframework.batch.core.StepExecutionListener#afterStep(StepExecution)
	 */
	public ExitStatus afterStep(StepExecution stepExecution) {
		ExitStatus status = null;
		for (Iterator iterator = list.reverse(); iterator.hasNext();) {
			StepExecutionListener listener = (StepExecutionListener) iterator.next();
			ExitStatus close = listener.afterStep(stepExecution);
			status = status != null ? status.and(close) : close;
		}
		return status;
	}

	/**
	 * Call the registered listeners in order, respecting and prioritising those
	 * that implement {@link Ordered}.
	 * @see org.springframework.batch.core.StepExecutionListener#beforeStep(StepExecution)
	 */
	public void beforeStep(StepExecution stepExecution) {
		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			StepExecutionListener listener = (StepExecutionListener) iterator.next();
			listener.beforeStep(stepExecution);
		}
	}

	/**
	 * Call the registered listeners in reverse order, respecting and
	 * prioritising those that implement {@link Ordered}.
	 * @see org.springframework.batch.core.StepExecutionListener#onErrorInStep(StepExecution,
	 * java.lang.Throwable)
	 */
	public ExitStatus onErrorInStep(StepExecution stepExecution, Throwable e) {
		ExitStatus status = null;
		for (Iterator iterator = list.reverse(); iterator.hasNext();) {
			StepExecutionListener listener = (StepExecutionListener) iterator.next();
			ExitStatus close = listener.onErrorInStep(stepExecution, e);
			status = status != null ? status.and(close) : close;
		}
		return status;
	}
}
