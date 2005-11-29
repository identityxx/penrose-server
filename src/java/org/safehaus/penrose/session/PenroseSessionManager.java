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
package org.safehaus.penrose.session;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.safehaus.penrose.Penrose;

public class PenroseSessionManager {
	
	public Logger log = Logger.getLogger(getClass());

	public List connectionPool = new ArrayList();
	public TreeMap activeConnections = new TreeMap();

    private Penrose penrose;

	public PenroseSessionManager(Penrose penrose) {
		this(penrose, 20);
	}

	public PenroseSessionManager(Penrose penrose, int size) {

        this.penrose = penrose;

		for (int i = 0; i < size; i++) {
			connectionPool.add(new PenroseSession(penrose));
		}
	}

    public synchronized PenroseSession createConnection() {
        //log.debug("Creating connection ...");
        PenroseSession session;

        if (connectionPool.size() > 0) {
            //log.debug("Got connection from pool.");
            session = (PenroseSession)connectionPool.remove(0);

        } else {
            //log.debug("Reuse oldest connection.");
            Date date = (Date)activeConnections.firstKey();
            session = (PenroseSession)activeConnections.remove(date);
        }

        session.setBindDn(null);
        session.setDate(new Date());

        activeConnections.put(session.getDate(), session);

        return session;
    }

    public synchronized void removeConnection(PenroseSession session) {
        //log.debug("Returned connection to pool.");
        activeConnections.remove(session.getDate());
        connectionPool.add(session);
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