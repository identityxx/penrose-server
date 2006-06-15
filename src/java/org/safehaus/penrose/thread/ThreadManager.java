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

import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Iterator;

public class ThreadManager {

    Logger log = Logger.getLogger(getClass());

	private Queue idleWorkers;
	private ThreadWorker[] workers;
	
	public ThreadManager(int numberOfThreads) {

        //log.debug("Creating ThreadManager("+numberOfThreads+")");

		// make sure that it is at least one
		numberOfThreads = Math.max(1, numberOfThreads);
		
		idleWorkers = new Queue();
		workers = new ThreadWorker[numberOfThreads];
		
		for (int i=0; i<workers.length; i++) {
			workers[i] = new ThreadWorker(idleWorkers);
		}
	}
	
	public void execute(Runnable runnable) throws InterruptedException {
        execute(runnable, log.isDebugEnabled());
    }

    public void execute(Runnable runnable, boolean foreground) throws InterruptedException {
        //log.info("Executing new thread");

        //String s = engineConfig.getParameter(EngineConfig.ALLOW_CONCURRENCY);
        //boolean allowConcurrency = s == null ? true : new Boolean(s).booleanValue();

        //if (threadManager == null || !allowConcurrency || log.isDebugEnabled()) {

        if (foreground) {
            runnable.run();

        } else {
            ThreadWorker worker = (ThreadWorker)idleWorkers.remove();
            worker.process(runnable);
        }
	}
	
	public void stopRequestIdleWorkers() {
        Collection c = idleWorkers.getAll();
        for (Iterator i=c.iterator(); i.hasNext(); ) {
            ((ThreadWorker)i.next()).stopRequest();
        }
	}
	
	public void stopRequestAllWorkers() {
		// Stop the idle one's first productive
		stopRequestIdleWorkers();
		
		// give the idle workers a quick chance to die
		try {
			Thread.sleep(250);
		} catch (InterruptedException ex) {
			// ignore
		}
		
		// Step through the list of all workers
		for (int i=0; i<workers.length; i++) {
			if (workers[i].isAlive()) {
				workers[i].stopRequest();
			}
		}
	}
	
	public void stopRequest() {
		// Stop the idle one's first productive
		stopRequestIdleWorkers();
		
		// give the idle workers a quick chance to die
		try {
			Thread.sleep(250);
		} catch (InterruptedException ex) {
			// ignore
		}
		
		// Step through the list of all workers
		for (int i=0; i<workers.length; i++) {
			if (workers[i].isAlive()) {
				workers[i].stopRequest();
			}
		}
	}
}
