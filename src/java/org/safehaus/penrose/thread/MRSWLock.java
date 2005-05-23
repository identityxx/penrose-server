/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.thread;

import org.apache.log4j.Logger;


/**
 * Multiple-Read-Single-Write lock
 * 
 * @author Administrator
 */
public class MRSWLock {

	static Logger logger = Logger.getLogger(MRSWLock.class);

	/**
	 * Internal integer value. Positive value is for read (no maximum or no
	 * limit to read lock given). Negative value is for write (min -1 or only 1
	 * write lock given).
	 */
	private int value;

	Queue queue;

    public MRSWLock() {
        queue = new Queue();
    }

	public MRSWLock(Queue queue) {
		this.queue = queue;
	}

	public synchronized void setValue(int newValue) {
		if (newValue != value) {
			value = newValue;
			notifyAll();
		}
	}

	public synchronized boolean getReadLock(long msTimeout)
			throws InterruptedException, TimeoutException {

        logger.debug("Getting read lock.");

        queue.add(Thread.currentThread().getName());

        boolean success = waitUntilValueIs(">=", 0, msTimeout);
		if (!success) throw new TimeoutException(this);

        setValue(value + 1);

        return true;
	}

	public synchronized boolean releaseReadLock(long msTimeout)
			throws InterruptedException, TimeoutException {

        logger.debug("Releasing read lock.");
		queue.remove();

        if (value > 0) {
            setValue(value - 1);
        }

        logger.debug("Read lock released.");

        return true;
	}

	public synchronized boolean getWriteLock(long msTimeout)
			throws InterruptedException, TimeoutException {

        logger.debug("Getting write lock.");

        queue.add(Thread.currentThread().getName());

		boolean success = waitUntilValueIs("==", 0, msTimeout);
        if (!success) throw new TimeoutException(this);

        setValue(value - 1);

		return success;
	}

	public synchronized boolean releaseWriteLock(long msTimeout)
			throws InterruptedException, TimeoutException {

        logger.debug("Releasing write lock.");

        setValue(value + 1);

        queue.remove();

        return true;
	}

	public synchronized int getValue() {
		return value;
	}

	public synchronized boolean waitUntilValueIs(String comparator, int value,
			long msTimeout) throws InterruptedException {

		//logger.debug("comparator=\"" + comparator + "\", value=" + value
		//		+ ", msTimeout=" + msTimeout);

		String threadName = Thread.currentThread().getName();
		queue.add(threadName);

		// Wait indefinitely if msTimeout == 0L
		if (msTimeout == 0L) {
			//logger.debug("this.value=" + this.value + ", value=" + value);
			while (!evaluate(comparator, value)
					|| !threadName.equals(queue.peek())) {
				wait(); // wait indefinitely until notified
			}
			// condition has finally been met
			queue.remove();
			return true;
		}

		// Otherwise, only wait for the specified amount of time
		long endTime = System.currentTimeMillis() + msTimeout;
		long msRemaining = msTimeout;
		boolean returnValue = false;

		//logger.debug("this.value=" + this.value + ", comparator=\""
		//		+ comparator + "\", value=" + value + ", msRemaining="
		//		+ msRemaining);
		while (!evaluate(comparator, value) && msRemaining > 0L
				&& !threadName.equals(queue.peek())) {
			wait(msRemaining);
			msRemaining = endTime - System.currentTimeMillis();
			//logger.debug("this.value=" + this.value + ", comparator=\""
			//		+ comparator + "\", value=" + value + ", msRemaining="
			//		+ msRemaining);
		}

		queue.remove();

		// may have timed out, or may have met value,
		// calculate return value
		returnValue = (this.value == value);
		return returnValue;
	}

	private boolean evaluate(String comparator, int value) {
		if ("==".equals(comparator)) {
			return this.value == value;
		} else if (">".equals(comparator)) {
			return this.value > value;
		} else if ("<".equals(comparator)) {
			return this.value < value;
		} else if ("!=".equals(comparator)) {
			return this.value != value;
		} else if (">=".equals(comparator)) {
			return this.value >= value;
		} else if ("<=".equals(comparator)) {
			return this.value <= value;
		} else {
			return false;
		}
	}

}