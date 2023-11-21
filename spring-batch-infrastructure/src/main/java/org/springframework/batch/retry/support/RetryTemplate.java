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

package org.springframework.batch.retry.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.retry.RetryCallback;
import org.springframework.batch.retry.RetryContext;
import org.springframework.batch.retry.RetryException;
import org.springframework.batch.retry.RetryListener;
import org.springframework.batch.retry.RetryOperations;
import org.springframework.batch.retry.RetryPolicy;
import org.springframework.batch.retry.TerminatedRetryException;
import org.springframework.batch.retry.backoff.BackOffContext;
import org.springframework.batch.retry.backoff.BackOffInterruptedException;
import org.springframework.batch.retry.backoff.BackOffPolicy;
import org.springframework.batch.retry.backoff.NoBackOffPolicy;
import org.springframework.batch.retry.policy.SimpleRetryPolicy;

/**
 * Template class that simplifies the execution of operations with retry
 * semantics. <br/> Retryable operations are encapsulated in implementations of
 * the {@link RetryCallback} interface and are executed using one of the
 * supplied {@link #execute} methods. <br/>
 * 
 * By default, an operation is retried if is throws any {@link Exception} or
 * subclass of {@link Exception}. This behaviour can be changed by using the
 * {@link #setRetryPolicy(RetryPolicy)} method. <br/>
 * 
 * Also by default, each operation is retried for a maximum of three attempts
 * with no back off in between. This behaviour can be configured using the
 * {@link #setRetryPolicy(RetryPolicy)} and
 * {@link #setBackOffPolicy(BackOffPolicy)} properties. The
 * {@link org.springframework.batch.retry.backoff.BackOffPolicy} controls how
 * long the pause is between each individual retry attempt. <br/>
 * 
 * This class is thread-safe and suitable for concurrent access when executing
 * operations and when performing configuration changes. As such, it is possible
 * to change the number of retries on the fly, as well as the
 * {@link BackOffPolicy} used and no in progress retryable operations will be
 * affected.
 * 
 * @author Rob Harrop
 * @author Dave Syer
 */
public class RetryTemplate implements RetryOperations {

	protected final Log logger = LogFactory.getLog(getClass());

	private volatile BackOffPolicy backOffPolicy = new NoBackOffPolicy();

	private volatile RetryPolicy retryPolicy = new SimpleRetryPolicy();

	private volatile RetryListener[] listeners = new RetryListener[0];

	/**
	 * Setter for listeners. The listeners are executed before and after a retry
	 * block (i.e. before and after all the attempts), and on an error (every
	 * attempt).
	 * @param listeners listeners
	 * @see RetryListener
	 */
	public void setListeners(RetryListener[] listeners) {
		this.listeners = listeners;
	}

	/**
	 * Register an additional listener.
	 * @param listener listener
	 * @see #setListeners(RetryListener[])
	 */
	public void registerListener(RetryListener listener) {
		List<RetryListener> list = new ArrayList<>(Arrays.asList(listeners));
		list.add(listener);
		listeners = list.toArray(new RetryListener[0]);
	}

	/**
	 * Setter for {@link BackOffPolicy}.
	 * @param backOffPolicy backOffPolicy
	 */
	public void setBackOffPolicy(BackOffPolicy backOffPolicy) {
		this.backOffPolicy = backOffPolicy;
	}

	/**
	 * Setter for {@link RetryPolicy}.
	 * 
	 * @param retryPolicy retryPolicy
	 */
	public void setRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	/**
	 * Keep executing the callback until it either succeeds or the policy
	 * dictates that we stop, in which case the most recent exception thrown by
	 * the callback will be rethrown.
	 * 
	 * @see org.springframework.batch.retry.RetryOperations#execute(org.springframework.batch.retry.RetryCallback)
	 * 
	 * @throws TerminatedRetryException if the retry has been manually
	 * terminated through the {@link RetryContext}.
	 */
	public final Object execute(RetryCallback callback) throws Exception {

		/*
		 * Read all needed data into local variables to prevent any
		 * reference/primitive changes on other threads affecting this retry
		 * attempt.
		 */
		BackOffPolicy backOffPolicy = this.backOffPolicy;
		RetryPolicy retryPolicy = this.retryPolicy;

		// Allow the retry policy to initialise itself...
		// TODO: catch and rethrow abnormal retry exception?
		RetryContext context = retryPolicy.open(callback, RetrySynchronizationManager.getContext());

		// Make sure the context is available globally for clients who need
		// it...
		RetrySynchronizationManager.register(context);

		Throwable lastException = null;

		try {

			// Give clients a chance to enhance the context...
			boolean running = doOpenInterceptors(callback, context);

			if (!running) {
				throw new TerminatedRetryException("Retry terminated abnormally by interceptor before first attempt");
			}

			// Start the backoff context...
			BackOffContext backOffContext = backOffPolicy.start(context);

			/*
			 * We allow the whole loop to be skipped if the policy or context
			 * already forbid the first try. This is used in the case of
			 * external retry to allow a recovery in handleRetryExhausted
			 * without the callback processing (which would throw an exception).
			 */
			while (retryPolicy.canRetry(context) && !context.isExhaustedOnly()) {

				try {
					logger.debug("Retry: count=" + context.getRetryCount());
					// Reset the last exception, so if we are successful
					// the close interceptors will not think we failed...
					lastException = null;
					return callback.doWithRetry(context);
				}
				catch (Throwable ex) {
					Throwable throwable = unwrapIfRethrown(ex);
					lastException = throwable;

					doOnErrorInterceptors(callback, context, throwable);

					retryPolicy.registerThrowable(context, throwable);

					if (retryPolicy.shouldRethrow(context)) {
						logger.debug("Rethrow in retry for policy: count=" + context.getRetryCount());
						rethrow(throwable);
					}

				}

				try {
					backOffPolicy.backOff(backOffContext);
				}
				catch (BackOffInterruptedException e) {
					lastException = e;
					// back off was prevented by another thread - fail the
					// retry
					logger.debug("Abort retry because interrupted: count=" + context.getRetryCount());
					rethrow(e);
				}

				/*
				 * A stateful policy that can retry should have rethrown the
				 * exception by now - i.e. we shouldn't get this far for a
				 * stateful policy if it can retry.
				 */
			}

			logger.debug("Retry failed last attempt: count=" + context.getRetryCount());
			return retryPolicy.handleRetryExhausted(context);

		}
		finally {
			retryPolicy.close(context);
			doCloseInterceptors(callback, context, lastException);
			RetrySynchronizationManager.clear();
		}
	}

	private boolean doOpenInterceptors(RetryCallback callback, RetryContext context) {

		boolean result = true;

		for (RetryListener listener : listeners) {
			result = result && listener.open(context, callback);
		}

		return result;

	}

	private void doCloseInterceptors(RetryCallback callback, RetryContext context, Throwable lastException) {
		for (int i = listeners.length; i-- > 0;) {
			listeners[i].close(context, callback, lastException);
		}
	}

	private void doOnErrorInterceptors(RetryCallback callback, RetryContext context, Throwable throwable) {
		for (int i = listeners.length; i-- > 0;) {
			listeners[i].onError(context, callback, throwable);
		}
	}

	/**
	 * Re-throw the exception directly if possible, wrap custom Throwables into
	 * {@link UnclassifiedRetryException}.
	 */
	private static void rethrow(Throwable throwable) throws Exception {
		if (throwable instanceof Exception) {
			throw (Exception) throwable;
		}
		else if (throwable instanceof Error) {
			throw (Error) throwable;
		}
		else {
			throw new UnclassifiedRetryException("Unclassified Throwable encountered", throwable);
		}
	}

	/**
	 * Undo the wrapping done in {@link #rethrow(Throwable)}
	 */
	private static Throwable unwrapIfRethrown(Throwable throwable) {
		if (throwable instanceof UnclassifiedRetryException) {
			return throwable.getCause();
		}
		else {
			return throwable;
		}
	}

	/**
	 * Runtime exception wrapper for Throwables that are neither Exception nor
	 * Error.
	 */
	private static class UnclassifiedRetryException extends RetryException {

		public UnclassifiedRetryException(String msg, Throwable cause) {
			super(msg, cause);
		}

	}

}
