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

import java.util.HashMap;

import org.springframework.batch.repeat.RepeatContext;
import org.springframework.batch.support.ExceptionClassifierSupport;

/**
 * Simple implementation of exception handler which looks for given exception
 * types. If one of the types is found then a counter is incremented and the
 * limit is checked to determine if it has been exceeded and the Throwable
 * should be re-thrown. Also allows to specify list of 'fatal' exceptions that
 * are never subject to counting, but are immediately re-thrown. The fatal list
 * has higher priority so the two lists needn't be exclusive.
 * 
 * @author Dave Syer
 * @author Robert Kasanicky
 */
public class SimpleLimitExceptionHandler implements ExceptionHandler {

	/**
	 * Name of exception classifier key for the nominated exception types.
	 */
	private static final String TX_INVALID = "TX_INVALID";

	/**
	 * Name of exception classifier key for the fatal exception types (not
	 * counted, immediately rethrown).
	 */
	private static final String FATAL = "FATAL";

	private RethrowOnThresholdExceptionHandler delegate = new RethrowOnThresholdExceptionHandler();

	private Class[] exceptionClasses = new Class[] { Exception.class };

	private Class[] fatalExceptionClasses = new Class[] { Error.class };

	/**
	 * Flag to indicate the the exception counters should be shared between
	 * sibling contexts in a nested batch (i.e. inner loop). Default is false.
	 * Set this flag to true if you want to count exceptions for the whole
	 * (outer) loop in a typical container.
	 * 
	 * @param useParent true if the parent context should be used to store the
	 * counters.
	 */
	public void setUseParent(boolean useParent) {
		delegate.setUseParent(useParent);
	}

	/**
	 * Convenience constructor for the {@link SimpleLimitExceptionHandler} to
	 * set the limit.
	 */
	public SimpleLimitExceptionHandler(int limit) {
		this();
		setLimit(limit);
	}

	/**
	 * Default constructor for the {@link SimpleLimitExceptionHandler}.
	 */
	public SimpleLimitExceptionHandler() {
		super();
		delegate.setExceptionClassifier(new ExceptionClassifierSupport() {
			public Object classify(Throwable throwable) {
				for (int i = 0; i < fatalExceptionClasses.length; i++) {
					if (fatalExceptionClasses[i].isAssignableFrom(throwable.getClass())) {
						return FATAL;
					}
				}
				for (int i = 0; i < exceptionClasses.length; i++) {
					if (exceptionClasses[i].isAssignableFrom(throwable.getClass())) {
						return TX_INVALID;
					}
				}
				return super.classify(throwable);
			}
		});
	}

	/**
	 * Rethrows only if the limit is breached for this context on the exception
	 * type specified.
	 * 
	 * @see #setExceptionClasses(Class[])
	 * @see #setLimit(int)
	 * 
	 * @see org.springframework.batch.repeat.exception.ExceptionHandler#handleException(org.springframework.batch.repeat.RepeatContext,
	 * Throwable)
	 */
	public void handleException(RepeatContext context, Throwable throwable) throws Throwable {
		delegate.handleException(context, throwable);
	}

	/**
	 * The limit on the given exception type within a single context before it
	 * is rethrown.
	 * 
	 * @param limit limit
	 */
	public void setLimit(final int limit) {
		delegate.setThresholds(new HashMap() {
			{
				put(ExceptionClassifierSupport.DEFAULT, new Integer(0));
				put(TX_INVALID, new Integer(limit));
				put(FATAL, new Integer(0));
			}
		});
	}

	/**
	 * Setter for the Throwable exceptionClasses that this handler counts.
	 * Defaults to {@link Exception}. If more exceptionClasses are specified
	 * handler uses single counter that is incremented when one of the
	 * recognized exception exceptionClasses is handled.
	 */
	public void setExceptionClasses(Class[] classes) {
		this.exceptionClasses = classes;
	}

	/**
	 * Setter for the Throwable exceptionClasses that shouldn't be counted, but
	 * rethrown immediately. This list has higher priority than
	 * {@link #setExceptionClasses(Class[])}.
	 * 
	 * @param fatalExceptionClasses defaults to {@link Error}
	 */
	public void setFatalExceptionClasses(Class[] fatalExceptionClasses) {
		this.fatalExceptionClasses = fatalExceptionClasses;
	}

}
