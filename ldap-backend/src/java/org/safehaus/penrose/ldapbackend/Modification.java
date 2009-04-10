package org.safehaus.penrose.ldapbackend;

/**
 * @author Endi S. Dewata
 */
public interface Modification {

    public final static int ADD     = 0;
    public final static int DELETE  = 1;
    public final static int REPLACE = 2;

    public void setType(int operation) throws Exception;
    public int getType() throws Exception;

    public void setAttribute(Attribute attribute) throws Exception;
    public Attribute getAttribute() throws Exception;
}
