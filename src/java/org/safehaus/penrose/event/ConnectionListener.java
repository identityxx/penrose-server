/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.event;

/**
 * @author Endi S. Dewata
 */
public interface ConnectionListener {
    public void connectionOpened(ConnectionEvent event) throws Exception;
    public void connectionClosed(ConnectionEvent event) throws Exception;
}
