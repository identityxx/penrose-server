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
package org.safehaus.penrose.client;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseConnection;

public class ClientManager {
	
	public Logger log = Logger.getLogger(getClass());

	public List connectionPool = new ArrayList();
	public TreeMap activeConnections = new TreeMap();

    private Penrose penrose;

	public ClientManager(Penrose penrose) {
		this(penrose, 20);
	}

	public ClientManager(Penrose penrose, int size) {

        this.penrose = penrose;

		for (int i = 0; i < size; i++) {
			connectionPool.add(new PenroseConnection(penrose));
		}
	}

    public synchronized PenroseConnection createConnection() {
        //log.debug("Creating connection ...");
        PenroseConnection connection;

        if (connectionPool.size() > 0) {
            //log.debug("Got connection from pool.");
            connection = (PenroseConnection)connectionPool.remove(0);

        } else {
            //log.debug("Reuse oldest connection.");
            Date date = (Date)activeConnections.firstKey();
            connection = (PenroseConnection)activeConnections.remove(date);
        }

        connection.setBindDn(null);
        connection.setDate(new Date());

        activeConnections.put(connection.getDate(), connection);

        return connection;
    }

    public synchronized void removeConnection(PenroseConnection connection) {
        //log.debug("Returned connection to pool.");
        activeConnections.remove(connection.getDate());
        connectionPool.add(connection);
    }

	public int getSize() {
		return connectionPool.size();
	}

    public Penrose getPenrose() {
        return penrose;
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
    }

}