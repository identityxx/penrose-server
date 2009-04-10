package org.safehaus.penrose.ldapbackend;

/**
 * @author Endi S. Dewata
 */
public interface Control {

    public String getOid() throws Exception;
    public byte[] getValue() throws Exception;
    public boolean isCritical() throws Exception;
}
