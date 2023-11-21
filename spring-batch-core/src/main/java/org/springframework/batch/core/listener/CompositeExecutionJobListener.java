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

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.core.Ordered;

/**
 * @author Dave Syer
 * 
 */
public class CompositeExecutionJobListener implements JobExecutionListener {

	private OrderedComposite listeners = new OrderedComposite();

	/**
	 * Public setter for the listeners.
	 * 
	 * @param listeners listeners
	 */
	public void setListeners(JobExecutionListener[] listeners) {
		this.listeners.setItems(listeners);
	}

	/**
	 * Register additional listener.
	 * 
	 * @param jobExecutionListener jobExecutionListener
	 */
	public void register(JobExecutionListener jobExecutionListener) {
		listeners.add(jobExecutionListener);
	}

	/**
	 * Call the registered listeners in reverse order, respecting and
	 * prioritising those that implement {@link Ordered}.
	 * @see org.springframework.batch.core.JobExecutionListener#afterJob(org.springframework.batch.core.JobExecution)
	 */
	public void afterJob(JobExecution jobExecution) {
		for (Iterator iterator = listeners.reverse(); iterator.hasNext();) {
			JobExecutionListener listener = (JobExecutionListener) iterator.next();
			listener.afterJob(jobExecution);
		}
	}

	/**
	 * Call the registered listeners in order, respecting and prioritising those
	 * that implement {@link Ordered}.
	 * @see org.springframework.batch.core.JobExecutionListener#beforeJob(org.springframework.batch.core.JobExecution)
	 */
	public void beforeJob(JobExecution jobExecution) {
		for (Iterator iterator = listeners.iterator(); iterator.hasNext();) {
			JobExecutionListener listener = (JobExecutionListener) iterator.next();
			listener.beforeJob(jobExecution);
		}
	}

	/**
	 * Call the registered listeners in reverse order, respecting and
	 * prioritising those that implement {@link Ordered}.
	 * @see org.springframework.batch.core.JobExecutionListener#onError(org.springframework.batch.core.JobExecution,
	 * java.lang.Throwable)
	 */
	public void onError(JobExecution jobExecution, Throwable e) {
		for (Iterator iterator = listeners.reverse(); iterator.hasNext();) {
			JobExecutionListener listener = (JobExecutionListener) iterator.next();
			listener.onError(jobExecution, e);
		}

	}

	/**
	 * Call the registered listeners in reverse order, respecting and
	 * prioritising those that implement {@link Ordered}.
	 * @see org.springframework.batch.core.JobExecutionListener#onInterrupt(org.springframework.batch.core.JobExecution)
	 */
	public void onInterrupt(JobExecution jobExecution) {
		for (Iterator iterator = listeners.reverse(); iterator.hasNext();) {
			JobExecutionListener listener = (JobExecutionListener) iterator.next();
			listener.onInterrupt(jobExecution);
		}

	}
}
