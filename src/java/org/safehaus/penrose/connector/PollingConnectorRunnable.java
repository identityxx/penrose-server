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
package org.safehaus.penrose.connector;

import org.apache.log4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class PollingConnectorRunnable implements Runnable {

    Logger log = Logger.getLogger(getClass());

	private PollingConnectorModule module;

    boolean running = true;

	public PollingConnectorRunnable(PollingConnectorModule module) {
		this.module = module;
	}

    public void run() {
        try {
            runImpl();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

	public void runImpl() throws Exception {

		while (running) {
            Thread.sleep(module.interval * 1000);
            if (running) module.process();
		}
		
	}

    public void stop() {
        running = false;
    }
}
