package org.safehaus.penrose.ldap;

/**
 * @author Endi S. Dewata
 */
public class CompareRequest extends Request implements Cloneable {

    protected DN dn;
    protected String attributeName;
    protected Object attributeValue;

    public DN getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = new DN(dn);
    }

    public void setDn(RDN rdn) throws Exception {
        this.dn = new DN(rdn);
    }

    public void setDn(DN dn) {
        this.dn = dn;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public Object getAttributeValue() {
        return attributeValue;
    }

    public void setAttributeValue(Object attributeValue) {
        this.attributeValue = attributeValue;
    }

    public void copy(CompareRequest request) {
        dn = request.dn;
        attributeName = request.attributeName;
        attributeValue = request.attributeValue;
    }

    public Object clone() throws CloneNotSupportedException {
        CompareRequest request = (CompareRequest)super.clone();
        request.copy(this);
        return request;
    }
}
