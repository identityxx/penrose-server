/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.event;

import org.safehaus.penrose.PenroseConnection;

/**
 * @author Endi S. Dewata
 */
public class SearchEvent extends Event {

    public final static int BEFORE_SEARCH = 0;
    public final static int AFTER_SEARCH  = 1;

    private PenroseConnection connection;
    private String base;
    private int returnCode;

    public SearchEvent(Object source, int type, PenroseConnection connection, String base) {
        super(source, type);
        this.connection = connection;
        this.base = base;
    }

    public PenroseConnection getConnection() {
        return connection;
    }

    public void setConnection(PenroseConnection connection) {
        this.connection = connection;
    }

    public String getBase() {
        return base;
    }

    public void setBase(String base) {
        this.base = base;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }
}
