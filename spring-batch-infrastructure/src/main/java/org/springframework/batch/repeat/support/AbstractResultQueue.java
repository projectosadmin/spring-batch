/*
 * Copyright 2002-2007 the original author or authors.
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

package org.springframework.batch.repeat.support;

import org.springframework.batch.repeat.RepeatException;

/**
 * Abstract implementation that can be extended for both Backport Concurrent and
 * JDK 5 Concurrent.
 * 
 * @author Ben Hale
 * @author Dave Syer
 */
abstract class AbstractResultQueue extends RepeatInternalStateSupport implements ResultQueue {

	// Arbitrary lock object.
	private final Object lock = new Object();

	// Counter to monitor the difference between expected and actually collected
	// results. When this reaches zero there are really no more results.
	private volatile int count = 0;

	public boolean isExpecting() {
		synchronized (lock) {
			// Base the decision about whether we expect more results on a
			// counter of the number of expected results actually collected.
			return count > 0;
		}
	}

	public void expect() {
		try {
			synchronized (lock) {
				aquireWait();
				count++;
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RepeatException("InterruptedException waiting for to acquire lock on input.");
		}
	}

	public void put(ResultHolder holder) {
		// There should be no need to block here, or to use offer()
		addResult(holder);
		// Take from the waits queue now to allow another result to
		// accumulate. But don't decrement the counter.
		releaseWait();
	}

	public ResultHolder take() {
		ResultHolder value;
		try {
			synchronized (lock) {
				value = takeResult();
				// Decrement the counter only when the result is collected.
				count--;
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RepeatException("InterruptedException while waiting for result.");
		}
		return value;
	}

	/**
	 * Acquire permission for one more task on the queue.
	 * 
	 * @throws InterruptedException InterruptedException
	 */
	protected abstract void aquireWait() throws InterruptedException;

	/**
	 * Release the permit that we were holding while a task was processed.
	 */
	protected abstract void releaseWait();

	/**
	 * Add a {@link ResultHolder} with a finished result to the queue for
	 * collection. Should not block. May throw an exception to signal that the
	 * queue is full, but that would be an unexpected condition.
	 * 
	 * @param resultHolder a {@link ResultHolder}
	 */
	protected abstract void addResult(ResultHolder resultHolder);

	/**
	 * Obtain a result from the queue, blocking until one becomes available.
	 * 
	 * @return a {@link ResultHolder} with the completed result
	 * @throws InterruptedException if an interrupt is signalled
	 */
	protected abstract ResultHolder takeResult() throws InterruptedException;

}
