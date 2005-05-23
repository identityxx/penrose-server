/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose;


import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.ArrayList;
import java.util.List;

import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseRemote;

/**
 * @author Endi S. Dewata
 */
public class PenroseRemoteObject extends UnicastRemoteObject implements PenroseRemote {

    public Penrose penrose;

    public PenroseRemoteObject(Penrose penrose) throws RemoteException {
        super();
        this.penrose = penrose;
    }

    public Config getConfig() throws RemoteException {
        return penrose.getConfig();
    }
    
    public int availableProcessors() throws RemoteException {
        Runtime rt = Runtime.getRuntime();
        return rt.availableProcessors();
    }

    public long freeMemory() throws RemoteException {
        Runtime rt = Runtime.getRuntime();
        return rt.freeMemory();
    }

    public long totalMemory() throws RemoteException {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory();
    }

    public long maxMemory() throws RemoteException {
        Runtime rt = Runtime.getRuntime();
        return rt.maxMemory();
    }

    public void gc() throws RemoteException {
        System.gc();
    }

    public void reloadConfig() throws RemoteException {
        try {
            penrose.loadConfig();

        } catch (Exception e) {
            throw new RemoteException(e.getMessage());
        }
    }

    public Collection getLdapConnections() throws RemoteException {
        try {
            List list = new ArrayList();
            //list.addAll(penrose.connectionPool.getAll());
            return list;

        } catch (Exception e) {
            throw new RemoteException(e.getMessage());
        }
    }
}
