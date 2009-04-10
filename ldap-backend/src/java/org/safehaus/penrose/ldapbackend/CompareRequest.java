package org.safehaus.penrose.ldapbackend;

/**
 * @author Endi S. Dewata
 */
public interface CompareRequest extends Request {

    public void setDn(DN dn) throws Exception;
    public DN getDn() throws Exception;

    public void setAttributeName(String name) throws Exception;
    public String getAttributeName() throws Exception;

    public void setAttributeValue(Object value) throws Exception;
    public Object getAttributeValue() throws Exception;
}
