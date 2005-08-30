/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.event;


import java.util.Collection;

import org.safehaus.penrose.PenroseConnection;

/**
 * @author Endi S. Dewata
 */
public class ModifyEvent extends Event {

    public final static int BEFORE_MODIFY = 0;
    public final static int AFTER_MODIFY  = 1;

    private PenroseConnection connection;
    private String dn;
    private int returnCode;

    private Collection modifications;

    public ModifyEvent(Object source, int type, PenroseConnection connection, String dn, Collection modifications) {
        super(source, type);
        this.connection = connection;
        this.dn = dn;
        this.modifications = modifications;
    }

    public String getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = dn;
    }

    public Collection getModifications() {
        return modifications;
    }

    public void setModifications(Collection modifications) {
        this.modifications = modifications;
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
