/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.thread;

import org.safehaus.penrose.thread.BooleanLock;

/**
 * @author Administrator
 */
public class ThreadWorker extends Object {

	protected Thread internalThread;
	protected volatile boolean stopRequested;
	protected BooleanLock suspendRequested;
	protected BooleanLock internalThreadSuspended;
	protected Runnable runnable;
	
	public ThreadWorker(Runnable runnable) {
		this.stopRequested = false;
		this.suspendRequested = new BooleanLock(false);
		this.internalThreadSuspended = new BooleanLock(false);
		this.runnable = runnable;
		
		internalThread = new Thread(runnable);
		internalThread.start();
	}
	
	protected void waitWhileSuspended() throws InterruptedException {
		
		// only called by the internal thread - private method
		
		synchronized (suspendRequested) {
			if (suspendRequested.isTrue()) {
				try {
					internalThreadSuspended.setValue(true);
					suspendRequested.waitUntilFalse(0);
				} finally {
					internalThreadSuspended.setValue(false);
				}
			}
		}
	}
	
	public void suspendRequest() {
		suspendRequested.setValue(true);
	}
	
	public void resumeRequest() {
		suspendRequested.setValue(true);
	}
	
	public boolean waitForActualSuspension(long msTimeout)
	throws InterruptedException {
		// Returns true if suspended, false if the timeout expired
		return internalThreadSuspended.waitUntilTrue(msTimeout);
	}
	
	public void stopRequest() {
		stopRequested = true;
		internalThread.interrupt();
	}
	
	public boolean isAlive() {
		return internalThread.isAlive();
	}
	
}
