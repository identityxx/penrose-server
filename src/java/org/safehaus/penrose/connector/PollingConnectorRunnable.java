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
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.PenroseServer;
import org.safehaus.penrose.Penrose;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PollingConnectorRunnable implements Runnable {

    Logger log = Logger.getLogger(getClass());

	private PollingConnectorService service;

    boolean running = true;

	public PollingConnectorRunnable(PollingConnectorService service) {
		this.service = service;
	}

    public void run() {
        try {
            runImpl();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

	public void runImpl() throws Exception {

        PenroseServer penroseServer = service.getPenroseServer();
        Penrose penrose = penroseServer.getPenrose();
        PartitionManager partitionManager = penrose.getPartitionManager();

        Collection partitions = new ArrayList();
        partitions.addAll(partitionManager.getPartitions());

		while (running) {

            Thread.sleep(service.interval * 1000);

            for (Iterator i=partitions.iterator(); i.hasNext(); ) {
                Partition partition = (Partition)i.next();
                service.process(partition);
            }
		}
		
	}

    public void stop() {
        running = false;
    }
}
