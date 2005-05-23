/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.event;

import org.safehaus.penrose.PenroseConnection;

/**
 * @author Endi S. Dewata
 */
public class DeleteEvent extends Event {

    public final static int BEFORE_DELETE = 0;
    public final static int AFTER_DELETE  = 1;

    private PenroseConnection connection;
    private int returnCode;

    private String dn;

    public DeleteEvent(Object source, int type, PenroseConnection connection, String dn) {
        super(source, type);
        this.connection = connection;
        this.dn = dn;
    }

    public PenroseConnection getConnection() {
        return connection;
    }

    public void setConnection(PenroseConnection connection) {
        this.connection = connection;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }

    public String getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = dn;
    }

}
