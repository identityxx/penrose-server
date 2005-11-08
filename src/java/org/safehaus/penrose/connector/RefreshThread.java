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
import org.safehaus.penrose.config.Config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class RefreshThread implements Runnable {

    Logger log = Logger.getLogger(getClass());

	private Connector connector;
	
	public RefreshThread(Connector connector) {
		this.connector = connector;
	}

	public void run() {
        //log.debug("RefreshThread has been started");

		while (!connector.isStopping()) {

			try {
                //Thread.sleep(2 * 60 * 1000); // sleep 2 minutes
				Thread.sleep(30 * 1000);

                Collection configs = new ArrayList();
                configs.addAll(connector.getConfigs());

                for (Iterator i=configs.iterator(); i.hasNext(); ) {
                    Config config = (Config)i.next();
                    connector.refresh(config);
                }

			} catch (Exception ex) {
				log.error(ex.getMessage(), ex);
			}
		}
		
	}

	

}
