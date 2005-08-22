/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.thread.ObjectFIFO;

/**
 * @author Administrator
 */
public class ThreadPoolWorker extends Object {

    Logger log = LoggerFactory.getLogger(getClass());

	private static int nextWorkerID = 0;
	
	private ObjectFIFO idleWorkers;
	private int workerID;
	private ObjectFIFO handoffBox;
	
	private Thread internalThread;
	private volatile boolean noStopRequested;
	
	public ThreadPoolWorker(ObjectFIFO idleWorkers) {
		this.idleWorkers = idleWorkers;
		this.workerID = getNextWorkerID();
		this.handoffBox = new ObjectFIFO(1); // only one slot
		
		// just before returning, the thread should be created
		noStopRequested = true;
		
		Runnable r = new Runnable() {
			public void run() {
				try {
					runWork();
				} catch (Exception ex) {
					// in case any exception slip through
					log.error(ex.getMessage(), ex);
				}
			}
		};
		
		String threadName = "ThreadPoolWorker-"+workerID;
		internalThread = new Thread(r, threadName);
		internalThread.start();
	}
	
	public static synchronized int getNextWorkerID() {
		// notice: synchronized at the class level to ensure uniqueness
		int id = nextWorkerID;
		nextWorkerID++;
		return id;
	}
	
	public void process(Runnable target) throws InterruptedException {
		handoffBox.add(target);
	}
	
	private void runWork() {
		while (noStopRequested) {
			try {
				String threadName = Thread.currentThread().getName();
				//log.debug("workerID="+workerID+" (threadName="+threadName+"), ready for work");
				// Worker is ready for work. 
				// This will never block because the idleWorker FIFO queue 
				// has enough capacity for all the workers
				idleWorkers.add(this);
				// wait here until the server adds a request
				Runnable r = (Runnable) handoffBox.remove();
				
				//log.debug("workerID="+workerID+", starting execution of new Runnable: "+r);
				runIt(r); // catches all exceptions
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt(); // re-assert
			}
		}
	}
	
	private void runIt(Runnable r) {
		try {
			r.run();
		} catch (Exception ex) {
			// catch any and all exceptions
			log.error("Uncaught exception fell through from run()", ex);
		} finally {
			// Clear the interrupted flag (in case it comes back set)
			// so that if the loop goes again, the handoffBox.remove() does not 
			// mistakenly throw an InterruptedException
			Thread.interrupted();
		}
	}
	
	public void stopRequest() {
		//log.debug("workerID="+workerID+", stopRequest() received");
		noStopRequested = false;
		internalThread.interrupt();
	}

	public boolean isAlive() {
		return internalThread.isAlive();
	}
}
