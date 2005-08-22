/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Administrator
 */
public class RefreshThread implements Runnable {

    Logger log = LoggerFactory.getLogger(getClass());

	private Engine engine;
	
	public RefreshThread(Engine penrose) {
		this.engine = penrose;
	}

	public void run() {

		try {
			// sleep 5 minutes first, to allow server initialization 
			Thread.sleep(5*60000);

		} catch (InterruptedException ex) {
			// ignore
		}

		while (!engine.isStopping()) {

			try {
				//handler.getSourceCache().refresh();
				Thread.sleep(2*60000); // sleep 2 minutes

			} catch (Exception ex) {
				log.error(ex.getMessage(), ex);
			}
		}
		
	}

	

}
