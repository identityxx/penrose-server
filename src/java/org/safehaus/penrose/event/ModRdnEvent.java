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
public class ModRdnEvent extends Event {

    public final static int BEFORE_MODRDN = 0;
    public final static int AFTER_MODRDN  = 1;

    private PenroseConnection connection;
    private LDAPEntry entry;
    private int returnCode;

    public ModRdnEvent(Object source, int type, PenroseConnection connection, LDAPEntry entry) {
        super(source, type);
        this.connection = connection;
        this.entry = entry;
    }

    public LDAPEntry getEntry() {
        return entry;
    }

    public void setEntry(LDAPEntry entry) {
        this.entry = entry;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }

    public PenroseConnection getConnection() {
        return connection;
    }

    public void setConnection(PenroseConnection connection) {
        this.connection = connection;
    }
}
