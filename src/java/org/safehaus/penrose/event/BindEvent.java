/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.event;

import org.safehaus.penrose.PenroseConnection;

/**
 * @author Endi S. Dewata
 */
public class BindEvent extends Event {

    public final static int BEFORE_BIND = 0;
    public final static int AFTER_BIND  = 1;

    private PenroseConnection connection;
    private String dn;
    private int returnCode;

    private String password;

    public BindEvent(Object source, int type, PenroseConnection connection, String dn, String password) {
        super(source, type);
        this.connection = connection;
        this.dn = dn;
        this.password = password;
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

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public PenroseConnection getConnection() {
        return connection;
    }

    public void setConnection(PenroseConnection connection) {
        this.connection = connection;
    }
}
