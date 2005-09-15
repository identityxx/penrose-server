/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PenroseConnectionPool implements PenroseConnectionPoolMBean {
	
	public Logger log = LoggerFactory.getLogger(getClass());

	public List connectionPool = new ArrayList();
	public TreeMap activeConnections = new TreeMap();

    private Penrose penrose;

	public PenroseConnectionPool(Penrose penrose) {
		this(penrose, 20);
	}

	public PenroseConnectionPool(Penrose penrose, int size) {

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