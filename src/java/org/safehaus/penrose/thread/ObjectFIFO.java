/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.thread;

/**
 * @author Administrator
 */
public class ObjectFIFO extends Object {

	private Object[] queue;
	private int capacity;
	private int size;
	private int head;
	private int tail;
	
	public ObjectFIFO(int capacity) {
		this.capacity = (capacity > 0) ? capacity : 1; // at least 1
		this.queue = new Object[capacity];
		this.head = 0;
		this.tail = 0;
		this.size = 0;
	}
	
	public int getCapacity() { return capacity; }
	
	public synchronized int getSize() { return size; }
	
	public synchronized boolean isEmpty() { return size==0; }
	
	public synchronized boolean isFull() { return size==capacity; }
	
	public synchronized void add(Object obj) throws InterruptedException {
		waitWhileFull();
		
		queue[head] = obj;
		head = (head+1) % capacity;
		size++;
		
		notifyAll(); // let any waiting threads know about this change
	}
	
	public synchronized void addEach(Object[] list) throws InterruptedException {
		// TODO code a more efficient implementation here
		for (int i=0; i<list.length; i++) {
			add(list[i]);
		}
	}
	
	public synchronized Object remove() throws InterruptedException {
		waitWhileEmpty();
		
		Object obj = queue[tail];
		
		// don't block GC by keeping unnecessary reference
		queue[tail] = null;
		
		tail = (tail+1) % capacity;
		size--;
		
		notifyAll(); // let any waiting threads know about this change
		
		return obj;
	}
	
	public synchronized Object[] removeAll() throws InterruptedException {
		// TODO code a more efficient implementation here
		Object[] list = new Object[size]; // use the current size
		
		for (int i=0; i<list.length; i++) {
			list[i] = remove();
		}
		
		// if FIFO was empty, a zero-length array is returned
		return list;
	}
	
	public synchronized Object[] removeAtLeastOne() throws InterruptedException {
		waitWhileEmpty(); // wait for at leats one to be in FIFO
		return removeAll();
	}
	
	public synchronized boolean waitUntilEmpty(long msTimeout) throws InterruptedException {
		if (msTimeout == 0L) {
			waitUntilEmpty(); // use other method
			return true;
		}
		
		// wait only for the specified amount of time
		long endTime = System.currentTimeMillis() + msTimeout;
		long msRemaining = msTimeout;
		
		while (!isEmpty() && msRemaining > 0L) {
			wait(msRemaining);
			msRemaining = endTime - System.currentTimeMillis();
		}
		
		// may have timed out, or may have met condition,
		// calculate return value
		return isEmpty();
	}
	
	public synchronized void waitUntilEmpty() throws InterruptedException {
		while (!isEmpty()) {
			wait();
		}
	}
	
	public synchronized void waitWhileEmpty() throws InterruptedException {
		while (isEmpty()) {
			wait();
		}
	}
	
	public synchronized void waitUntilFull() throws InterruptedException {
		while (!isFull()) {
			wait();
		}
	}
	
	public synchronized void waitWhileFull() throws InterruptedException {
		while (isFull()) {
			wait();
		}
	}
}
