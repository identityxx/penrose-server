/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.thread;

import java.util.Vector;

import org.apache.log4j.Logger;

/**
 * @author Administrator
 */
public class Queue {
	
	protected Vector queue = new Vector();
    Logger log = Logger.getLogger(getClass());
	
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
