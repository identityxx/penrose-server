/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.thread;


/**
 * @author Administrator
 */
public class TimeoutException extends Exception {
	
	String msg;
	
	public TimeoutException(MRSWLock lock) {
		msg = "currentThread="+Thread.currentThread().getName()
		+", lockValue="+lock.getValue()
		+", queueHead="+(lock.queue.isEmpty()? "null (empty queue)" : lock.queue.peek());
	}
	
	public String getMessage() {
		return msg;
	}

}
