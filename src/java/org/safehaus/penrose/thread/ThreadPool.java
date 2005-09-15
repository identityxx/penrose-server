/** * Copyright (c) 2000-2005, Identyx Corporation.
 * 
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */package org.safehaus.penrose.thread;import org.safehaus.penrose.thread.ObjectFIFO;/** * @author Administrator */public class ThreadPool implements ThreadPoolMBean {	private ObjectFIFO idleWorkers;	private ThreadPoolWorker[] workerList;		public ThreadPool(int numberOfThreads) {		// make sure that it is at least one		numberOfThreads = Math.max(1, numberOfThreads);				idleWorkers = new ObjectFIFO(numberOfThreads);		workerList = new ThreadPoolWorker[numberOfThreads];				for (int i=0; i<workerList.length; i++) {			workerList[i] = new ThreadPoolWorker(idleWorkers);		}	}		public void execute(Runnable target) throws InterruptedException {		// block (forever) until a worker is avaiable		ThreadPoolWorker worker = (ThreadPoolWorker) idleWorkers.remove();		worker.process(target);	}		public void stopRequestIdleWorkers() {		try {			Object[] idle = idleWorkers.removeAll();			for (int i=0; i<idle.length; i++) {				((ThreadPoolWorker) idle[i]).stopRequest();			}		} catch (InterruptedException ex) {			Thread.currentThread().interrupt(); // re-assert		}	}		public void stopRequestAllWorkers() {		// Stop the idle one's first productive		stopRequestIdleWorkers();				// give the idle workers a quick chance to die		try {			Thread.sleep(250);		} catch (InterruptedException ex) {			// ignore		}				// Step through the list of all workers		for (int i=0; i<workerList.length; i++) {			if (workerList[i].isAlive()) {				workerList[i].stopRequest();			}		}	}		public void stopRequest() {		// Stop the idle one's first productive		stopRequestIdleWorkers();				// give the idle workers a quick chance to die		try {			Thread.sleep(250);		} catch (InterruptedException ex) {			// ignore		}				// Step through the list of all workers		for (int i=0; i<workerList.length; i++) {			if (workerList[i].isAlive()) {				workerList[i].stopRequest();			}		}	}}