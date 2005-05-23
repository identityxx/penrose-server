/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose;


import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Collection;

import org.safehaus.penrose.config.Config;

/**
 * @author Endi S. Dewata
 */
public interface PenroseRemote extends Remote {

    public final static String NAME = "Penrose";

    public Config getConfig() throws RemoteException;
    public int availableProcessors() throws RemoteException;
    public long freeMemory() throws RemoteException;
    public long totalMemory() throws RemoteException;
    public long maxMemory() throws RemoteException;
    public void gc() throws RemoteException;

    public void reloadConfig() throws RemoteException;
    public Collection getLdapConnections() throws RemoteException;
}
