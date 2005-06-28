/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.thread;

import java.util.Vector;

import org.apache.log4j.Logger;
import org.safehaus.penrose.thread.EmptyQueueException;

/**
 * @author Administrator
 */
public class Queue {
	
	protected Vector queue = new Vector();
	protected Logger logger = Logger.getLogger(Queue.class);
	
	public synchronized boolean isEmpty() {
		return queue.size() == 0;
	}
	
	/**
	 * Adds an object to the tail of the queue
	 * 
	 * @param obj
	 */
	public synchronized void add(Object obj) {
		//logger.debug("Adding: "+obj+" to "+toString());
		queue.add(obj);
	}
	
	/**
	 * Remove an object from the head of the queue
	 * 
	 * @return the object
	 * @throws EmptyQueueException if the queue is empty
	 */
	public synchronized Object remove() throws EmptyQueueException {
		//logger.debug("Removing: "+toString());
		if (isEmpty()) throw new EmptyQueueException();
		return queue.remove(0);
	}
	
	/**
	 * Peek an object from the head of the queue
	 * 
	 * @return the object
	 * @throws EmptyQueueException if the queue is empty
	 */
	public synchronized Object peek() throws EmptyQueueException {
		//logger.debug("Content: "+toString());
		if (isEmpty()) throw new EmptyQueueException();
		return queue.get(0);
	}
	
	/**
	 * Remove the object in the queue if it's the same as the object passed as parameter 
	 * 
	 * @param obj 
	 * @return whether the object in the queue is removed
	 * @throws EmptyQueueException if the queue is empty
	 */
	public synchronized boolean removeIfSame(Object obj) throws EmptyQueueException {
		//logger.debug(toString());
		if (isEmpty()) throw new EmptyQueueException();
		Object obj2 = queue.get(0);
		if (obj2.equals(obj)) {
			queue.remove(0);
			return true;
		}
		return false;
	}
	
	/**
	 * Dump the queue content
	 */
	public String toString() {
		StringBuffer sb = new StringBuffer("Queue content: ");
		for (int i=0; i<queue.size(); i++) {
			sb.append(queue.elementAt(i)+";");
		}
		return sb.toString();
	}

}
