/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class ThreadWorker extends Object {

    Logger log = LoggerFactory.getLogger(getClass());

    private static int nextWorkerID = 0;

    private Queue idleWorkers;
    private Queue pendingJobs;

    private int id;

    private Thread internalThread;
    private volatile boolean noStopRequested;

    public ThreadWorker(Queue idleWorkers) {
        this.idleWorkers = idleWorkers;
        this.id = getNextWorkerID();
        this.pendingJobs = new Queue(); // only one slot

        // just before returning, the thread should be created
        noStopRequested = true;

        Runnable r = new Runnable() {
            public void run() {
                try {
                    runJobs();
                } catch (Exception ex) {
                    // in case any exception slip through
                    log.error(ex.getMessage(), ex);
                }
            }
        };

        String threadName = "ThreadWorker-"+id;
        internalThread = new Thread(r, threadName);
        internalThread.start();
    }

    public static synchronized int getNextWorkerID() {
        // notice: synchronized at the class level to ensure uniqueness
        int id = nextWorkerID;
        nextWorkerID++;
        return id;
    }

    public void process(Runnable job) throws InterruptedException {
        pendingJobs.add(job);
    }

    private void runJobs() {
        while (noStopRequested) {
            //try {
                //String threadName = Thread.currentThread().getName();
                //log.debug("workerID="+workerID+" (threadName="+threadName+"), ready for work");
                // Worker is ready for work.
                // This will never block because the idleWorker FIFO queue
                // has enough capacity for all the workers
                idleWorkers.add(this);
                // wait here until the server adds a request
                Runnable r = (Runnable)pendingJobs.remove();

                //log.debug("workerID="+workerID+", starting execution of new Runnable: "+r);
                runIt(r); // catches all exceptions
            //} catch (InterruptedException ex) {
            //	Thread.currentThread().interrupt(); // re-assert
            //}
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
            // so that if the loop goes again, the pendingJobs.remove() does not
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

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
