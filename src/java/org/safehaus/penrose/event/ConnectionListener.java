/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
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
