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

package org.springframework.batch.repeat.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.repeat.CompletionPolicy;
import org.springframework.batch.repeat.ExitStatus;
import org.springframework.batch.repeat.RepeatCallback;
import org.springframework.batch.repeat.RepeatContext;
import org.springframework.batch.repeat.RepeatException;
import org.springframework.batch.repeat.RepeatListener;
import org.springframework.batch.repeat.RepeatOperations;
import org.springframework.batch.repeat.exception.DefaultExceptionHandler;
import org.springframework.batch.repeat.exception.ExceptionHandler;
import org.springframework.batch.repeat.policy.DefaultResultCompletionPolicy;
import org.springframework.util.Assert;

/**
 * Simple implementation and base class for batch templates implementing
 * {@link RepeatOperations}. Provides a framework including interceptors and
 * policies. Subclasses just need to provide a method that gets the next result
 * and one that waits for all the results to be returned from concurrent
 * processes or threads.<br/>
 * 
 * N.B. the template accumulates thrown exceptions during the iteration, and
 * they are all processed together when the main loop ends (i.e. finished
 * processing the items). Clients that do not want to stop execution when an
 * exception is thrown can use a specific {@link CompletionPolicy} that does not
 * finish when exceptions are received. This is not the default behaviour.<br/>
 * 
 * Clients that want to take some business action when an exception is thrown by
 * the {@link RepeatCallback} can consider using a custom {@link RepeatListener}
 * instead of trying to customise the {@link CompletionPolicy}. This is
 * generally a friendlier interface to implement, and the
 * {@link RepeatListener#after(RepeatContext, ExitStatus)} method is passed in
 * the result of the callback, which would be an instance of {@link Throwable}
 * if the business processing had thrown an exception. If the exception is not
 * to be propagated to the caller, then a non-default {@link CompletionPolicy}
 * needs to be provided as well, but that could be off the shelf, with the
 * business action implemented only in the interceptor.
 * 
 * @author Dave Syer
 * 
 */
public class RepeatTemplate implements RepeatOperations {

	protected Log logger = LogFactory.getLog(getClass());

	private RepeatListener[] listeners = new RepeatListener[] {};

	private CompletionPolicy completionPolicy = new DefaultResultCompletionPolicy();

	private ExceptionHandler exceptionHandler = new DefaultExceptionHandler();

	/**
	 * Set the listeners for this template, registering them for callbacks at
	 * appropriate times in the iteration.
	 * 
	 * @param listeners listeners
	 */
	public void setListeners(RepeatListener[] listeners) {
		this.listeners = listeners;
	}

	/**
	 * Register an additional listener.
	 * 
	 * @param listener listener
	 */
	public void registerListener(RepeatListener listener) {
		List list = new ArrayList(Arrays.asList(listeners));
		list.add(listener);
		listeners = (RepeatListener[]) list.toArray(new RepeatListener[list.size()]);
	}

	/**
	 * Setter for exception handler strategy. The exception handler is called at
	 * the end of a batch, after the {@link CompletionPolicy} has determined
	 * that the batch is complete. By default all exceptions are re-thrown.
	 * 
	 * @see ExceptionHandler
	 * @see DefaultExceptionHandler
	 * @see #setCompletionPolicy(CompletionPolicy)
	 * 
	 * @param exceptionHandler the {@link ExceptionHandler} to use.
	 */
	public void setExceptionHandler(ExceptionHandler exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
	}

	/**
	 * Setter for policy to decide when the batch is complete. The default is to
	 * complete normally when the callback returns a {@link ExitStatus} which is
	 * not marked as continuable, and abnormally when the callback throws an
	 * exception (but the decision to re-throw the exception is deferred to the
	 * {@link ExceptionHandler}).
	 * 
	 * @see #setExceptionHandler(ExceptionHandler)
	 * 
	 * @param terminationPolicy a TerminationPolicy.
	 * @throws IllegalArgumentException if the argument is null
	 */
	public void setCompletionPolicy(CompletionPolicy terminationPolicy) {
		Assert.notNull(terminationPolicy);
		this.completionPolicy = terminationPolicy;
	}

	/**
	 * Execute the batch callback until the completion policy decides that we
	 * are finished. Wait for the whole batch to finish before returning even if
	 * the task executor is asynchronous.
	 * 
	 * @see org.springframework.batch.repeat.RepeatOperations#iterate(org.springframework.batch.repeat.RepeatCallback)
	 */
	public ExitStatus iterate(RepeatCallback callback) {

		RepeatContext outer = RepeatSynchronizationManager.getContext();

		ExitStatus result = ExitStatus.CONTINUABLE;
		try {
			// This works with an asynchronous TaskExecutor: the
			// interceptors have to wait for the child processes.
			result = executeInternal(callback);
		}
		finally {
			RepeatSynchronizationManager.clear();
			if (outer != null) {
				RepeatSynchronizationManager.register(outer);
			}
		}

		return result;
	}

	/**
	 * Internal convenience method to loop over interceptors and batch
	 * callbacks.
	 * 
	 * @param callback the callback to process each element of the loop.
	 * 
	 * @return the aggregate of {@link ContinuationPolicy#canContinue(Object)}
	 * for all the results from the callback.
	 * 
	 */
	private ExitStatus executeInternal(final RepeatCallback callback) {

		// Reset the termination policy if there is one...
		RepeatContext context = start();

		// Make sure if we are already marked complete before we start then no
		// processing takes place.
		boolean running = !isMarkedComplete(context);

		for (int i = 0; i < listeners.length; i++) {
			RepeatListener interceptor = listeners[i];
			interceptor.open(context);
			running = running && !isMarkedComplete(context);
			if (!running)
				break;
		}

		// Return value, default is to allow continued processing.
		ExitStatus result = ExitStatus.CONTINUABLE;

		RepeatInternalState state = createInternalState(context);
		Collection throwables = state.getThrowables();

		try {

			while (running) {

				/*
				 * Run the before interceptors here, not in the task executor so
				 * that they all happen in the same thread - it's easier for
				 * tracking batch status, amongst other things.
				 */
				for (int i = 0; i < listeners.length; i++) {
					RepeatListener interceptor = listeners[i];
					interceptor.before(context);
					// Allow before interceptors to veto the batch by setting
					// flag.
					running = running && !isMarkedComplete(context);
				}

				// Check that we are still running...
				if (running) {

					logger.debug("Repeat operation about to start at count=" + context.getStartedCount());

					try {

						result = getNextResult(context, callback, state);
						executeAfterInterceptors(context, result);

					}
					catch (Throwable throwable) {

						// An exception alone is not sufficient grounds for not
						// continuing
						Throwable unwrappedThrowable = unwrapIfRethrown(throwable);
						try {

							for (int i = listeners.length; i-- > 0;) {
								RepeatListener interceptor = listeners[i];
								interceptor.onError(context, unwrappedThrowable);
								// This is not an error - only log at debug
								// level.
								logger.debug("Exception intercepted (" + (i + 1) + " of " + listeners.length + ")",
										unwrappedThrowable);
							}

							exceptionHandler.handleException(context, unwrappedThrowable);

						}
						catch (Throwable handled) {
							throwables.add(handled);
						}
					}

					// N.B. the order may be important here:
					if (isComplete(context, result) || isMarkedComplete(context) || !throwables.isEmpty()) {
						running = false;
					}
				}

			}

			result = result.and(waitForResults(state));

			// Explicitly drop any references to internal state...
			state = null;

		}
		/*
		 * No need for explicit catch here - if the business processing threw an
		 * exception it was already handled by the helper methods. An exception
		 * here is necessarily fatal.
		 */
		finally {

			try {

				if (!throwables.isEmpty()) {
					rethrow((Throwable) throwables.iterator().next());
				}

			}
			finally {

				try {
					for (int i = listeners.length; i-- > 0;) {
						RepeatListener interceptor = listeners[i];
						interceptor.close(context);
					}
				}
				finally {
					// TODO: extend this to the completion policy?
					context.close();
				}

			}

		}

		return result;

	}

	/**
	 * Re-throws the original throwable if it is unchecked, wraps checked
	 * exceptions into {@link RepeatException}.
	 */
	private static void rethrow(Throwable throwable) throws RuntimeException {
		if (throwable instanceof Error) {
			throw (Error) throwable;
		}
		else if (throwable instanceof RuntimeException) {
			throw (RuntimeException) throwable;
		}
		else {
			throw new RepeatException("Exception in batch process", throwable);
		}
	}

	/**
	 * Unwraps the throwable if it has been wrapped by
	 * {@link #rethrow(Throwable)}.
	 */
	private static Throwable unwrapIfRethrown(Throwable throwable) {
		if (throwable instanceof RepeatException) {
			return throwable.getCause();
		}
		else {
			return throwable;
		}
	}

	/**
	 * Create an internal state object that is used to store data needed
	 * internally in the scope of an iteration. Used by subclasses to manage the
	 * queueing and retrieval of asynchronous results. The default just provides
	 * an accumulation of Throwable instances for processing at the end of the
	 * batch.
	 * 
	 * @param context the current {@link RepeatContext}
	 * @return a {@link RepeatInternalState} instance.
	 */
	protected RepeatInternalState createInternalState(RepeatContext context) {
		return new RepeatInternalStateSupport();
	}

	/**
	 * Get the next completed result, possibly executing several callbacks until
	 * one finally finishes.
	 * 
	 * @param context current BatchContext.
	 * @param callback the callback to execute.
	 * @param state maintained by the implementation.
	 * @return a finished result.
	 * 
	 * @see #isComplete(RepeatContext)
	 */
	protected ExitStatus getNextResult(RepeatContext context, RepeatCallback callback, RepeatInternalState state)
			throws Throwable {
		update(context);
		return callback.doInIteration(context);

	}

	/**
	 * If necessary, wait for results to come back from remote or concurrent
	 * processes. By default does nothing and returns true.
	 * 
	 * @param state the internal state.
	 * @return true if {@link #canContinue(ExitStatus)} is true for all results
	 * retrieved.
	 */
	protected boolean waitForResults(RepeatInternalState state) {
		// no-op by default
		return true;
	}

	/**
	 * Check return value from batch operation.
	 * 
	 * @param value the last callback result.
	 * @return true if the value is {@link ExitStatus#CONTINUABLE}.
	 */
	protected final boolean canContinue(ExitStatus value) {
		return ((ExitStatus) value).isContinuable();
	}

	private boolean isMarkedComplete(RepeatContext context) {
		boolean complete = context.isCompleteOnly();
		if (context.getParent() != null) {
			complete = complete || isMarkedComplete(context.getParent());
		}
		if (complete) {
			logger.debug("Repeat is complete according to context alone.");
		}
		return complete;

	}

	/**
	 * Convenience method to execute after interceptors on a callback result.
	 * 
	 * @param context the current batch context.
	 * @param value the result of the callback to process.
	 */
	protected void executeAfterInterceptors(final RepeatContext context, ExitStatus value) {

		// Don't re-throw exceptions here: let the exception handler deal with
		// that...

		if (value != null && value.isContinuable()) {
			for (int i = listeners.length; i-- > 0;) {
				RepeatListener interceptor = listeners[i];
				interceptor.after(context, value);
			}

		}

	}

	/**
	 * Delegate to the {@link CompletionPolicy}.
	 * 
	 * @see org.springframework.batch.repeat.CompletionPolicy#isComplete(RepeatContext,
	 * ExitStatus)
	 */
	protected boolean isComplete(RepeatContext context, ExitStatus result) {
		boolean complete = completionPolicy.isComplete(context, result);
		if (complete) {
			logger.debug("Repeat is complete according to policy and result value.");
		}
		return complete;
	}

	/**
	 * Delegate to {@link CompletionPolicy}.
	 * 
	 * @see org.springframework.batch.repeat.CompletionPolicy#isComplete(RepeatContext)
	 */
	protected boolean isComplete(RepeatContext context) {
		boolean complete = completionPolicy.isComplete(context);
		if (complete) {
			logger.debug("Repeat is complete according to policy alone not including result.");
		}
		return complete;
	}

	/**
	 * Delegate to the {@link CompletionPolicy}.
	 * 
	 * @see org.springframework.batch.repeat.CompletionPolicy#start(RepeatContext)
	 */
	protected RepeatContext start() {
		RepeatContext parent = RepeatSynchronizationManager.getContext();
		RepeatContext context = completionPolicy.start(parent);
		RepeatSynchronizationManager.register(context);
		logger.debug("Starting repeat context.");
		return context;
	}

	/**
	 * Delegate to the {@link CompletionPolicy}.
	 * 
	 * @see org.springframework.batch.repeat.CompletionPolicy#update(RepeatContext)
	 */
	protected void update(RepeatContext context) {
		completionPolicy.update(context);
	}

}
