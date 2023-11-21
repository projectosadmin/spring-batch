package org.springframework.batch.integration.chunk;

import org.springframework.batch.item.ItemWriterException;

/**
 * Exception indicating that a failure or early completion condition was
 * detected in a remote worker.
 * 
 * @author Dave Syer
 * 
 */
public class AsynchronousFailureException extends ItemWriterException {

	/**
	 * Create a new {@link AsynchronousFailureException} based on a message and
	 * another exception.
	 * 
	 * @param message message
	 *            the message for this exception
	 * @param cause cause
	 *            the other exception
	 */
	public AsynchronousFailureException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Create a new {@link AsynchronousFailureException} based on a message.
	 * 
	 * @param message message
	 *            the message for this exception
	 */
	public AsynchronousFailureException(String message) {
		super(message);
	}

}
