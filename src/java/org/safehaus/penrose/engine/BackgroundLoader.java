/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import java.util.Hashtable;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.thread.NormalThread;


/**
 * BackgroundLoader 
 * 
 * - Singleton pattern
 * - This Runnable class is responsible to load the cache tables 
 * 
 * @author Administrator
 */
public class BackgroundLoader extends NormalThread implements Runnable {

    Logger log = LoggerFactory.getLogger(getClass());

	final static int WAKE_UP_INTERVAL = 1000; // every second
	final static boolean DEBUG = true;
	
	final static int TABLE_BLOCKED = 1;
	final static int TABLE_DIRTY = 2;

	static BackgroundLoader instance = null;
	
	public static BackgroundLoader getInstance() {
		return instance;
	}
	
	public static BackgroundLoader createInstance(Penrose penrose) throws Exception {
		instance = new BackgroundLoader(penrose);
		return instance;
	}
	
	private Penrose penrose;
	private Vector updaterQueue;

	/**
	 * The table status (whether blocked or not). We use Hashtable for thread safety.
	 */
	private Hashtable tableStatus = new Hashtable();
	
	/**
	 * Construct the background loader
	 * 
	 * @param penrose
	 * @throws Exception
	 */
	public BackgroundLoader(Penrose penrose) throws Exception {
		super();
		this.penrose = penrose;
	}
	
	/**
	 * Runner
	 */
	public void run() {
		try {
			while (true) {
				Thread.sleep(WAKE_UP_INTERVAL);
				log.debug("test");
				
			}
		} catch (InterruptedException ex) {
			log.error(ex.getMessage(), ex);
		}
	}

    /**
     * Get the lock for performing update
     */
/*
    public void getLockForUpdate(Source source) {
        SourceDefinition sourceConfig = (SourceDefinition)penrose.getConfig().getSources().get(source.getName());
    	String tableName = penrose.getSourceCache().getEntryTableName(sourceConfig, false);
    	Boolean locked = (Boolean)tableStatus.get(tableName);
    	String threadId = Thread.currentThread().toString();
    	updaterQueue.add(threadId);
    	
    	if (locked == Boolean.TRUE && threadId.equals((String)updaterQueue.get(0))) {
    		try {
    			wait();
    		} catch (InterruptedException ex) {}
    	}
    	notifyAll();
    }
*/
    /**
     * Release the lock after performing update
     */
/*
    public void releaseLockForUpdate(Source source) {
        SourceDefinition sourceConfig = (SourceDefinition)penrose.getConfig().getSources().get(source.getName());
    	String tableName = penrose.getSourceCache().getEntryTableName(sourceConfig, false);
    	String threadId = Thread.currentThread().toString();
    	if (threadId.equals((String)updaterQueue.get(0))) {
    		updaterQueue.remove(0);
    	} else {
    		log.debug("Error: threadId="+threadId+", but updaterQueue[0]="+updaterQueue.get(0).toString());
    	}
    	notifyAll();
    }
*/
	protected void runWork() {
		
	}
	
}
