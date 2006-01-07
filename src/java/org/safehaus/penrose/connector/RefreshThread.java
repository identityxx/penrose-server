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
import org.safehaus.penrose.partition.Partition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class RefreshThread implements Runnable {

    Logger log = Logger.getLogger(getClass());

	private Connector connector;
    private ConnectorConfig connectorConfig;

    int refreshInterval;

	public RefreshThread(Connector connector) {
		this.connector = connector;
        this.connectorConfig = connector.getConnectorConfig();

        String s = connectorConfig.getParameter(ConnectorConfig.REFRESH_INTERVAL);
        refreshInterval = s == null ? ConnectorConfig.DEFAULT_REFRESH_INTERVAL : Integer.parseInt(s);
	}

	public void run() {
/*
        try {
            // sleep 5 minutes first, to allow server initialization
            Thread.sleep(5*60000);

        } catch (InterruptedException ex) {
            // ignore
        }
*/
		while (!connector.isStopping()) {

			try {
				Thread.sleep(refreshInterval * 1000);

                Collection configs = new ArrayList();
                configs.addAll(connector.getPartitionManager().getPartitions());

                for (Iterator i=configs.iterator(); i.hasNext(); ) {
                    Partition partition = (Partition)i.next();
                    connector.refresh(partition);
                }

            } catch (InterruptedException e) {
                // ignore
                
			} catch (Exception e) {
                log.error(e.getMessage(), e);
			}
		}
		
	}

	

}
