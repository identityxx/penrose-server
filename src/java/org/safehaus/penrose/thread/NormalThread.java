/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.thread;

import org.apache.log4j.Logger;
import org.safehaus.penrose.thread.BooleanLock;

/**
 * @author Administrator
 */
public abstract class NormalThread extends Object {

	private Logger log = Logger.getLogger(getClass());

	protected Thread internalThread;
	protected volatile boolean stopRequested;
	protected BooleanLock suspendRequested;
	protected BooleanLock internalThreadSuspended;
	
	public NormalThread() {
		stopRequested = false;
		suspendRequested = new BooleanLock(false);
		internalThreadSuspended = new BooleanLock(false);
		
		Runnable r = new Runnable() {
			public void run() {
				try {
					runWork();
				} catch (Exception ex) {
					// in case any exception slips through
					log.error(ex.getMessage(), ex);
				}
			}
		};
		
		internalThread = new Thread(r);
		internalThread.start();
	}
	
	abstract protected void runWork(); 
	
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
