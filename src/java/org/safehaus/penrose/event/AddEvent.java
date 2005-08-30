/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.event;

import org.safehaus.penrose.PenroseConnection;
import org.ietf.ldap.LDAPEntry;

/**
 * @author Endi S. Dewata
 */
public class AddEvent extends Event {

    public final static int BEFORE_ADD = 0;
    public final static int AFTER_ADD  = 1;

    private PenroseConnection connection;
    private LDAPEntry entry;
    private int returnCode;

    public AddEvent(Object source, int type, PenroseConnection connection, LDAPEntry entry) {
        super(source, type);
        this.connection = connection;
        this.entry = entry;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }

    public LDAPEntry getEntry() {
        return entry;
    }

    public void setEntry(LDAPEntry entry) {
        this.entry = entry;
    }

    public PenroseConnection getConnection() {
        return connection;
    }

    public void setConnection(PenroseConnection connection) {
        this.connection = connection;
    }
}
