package org.safehaus.penrose.ldap;

/**
 * @author Endi S. Dewata
 */
public class AddRequest extends Request implements Cloneable {

    protected DN dn;
    protected Attributes attributes;

    public AddRequest() {
    }

    public AddRequest(AddRequest request) {
        super(request);
        dn = request.getDn();
        attributes = request.getAttributes();
    }

    public DN getDn() {
        return dn;
    }

    public void setDn(String dn) throws Exception {
        this.dn = new DN(dn);
    }

    public void setDn(RDN rdn) throws Exception {
        this.dn = new DN(rdn);
    }

    public void setDn(DN dn) {
        this.dn = dn;
    }

    public Attributes getAttributes() {
        return attributes;
    }

    public void setAttributes(Attributes attributes) {
        this.attributes = attributes;
    }

    public Object clone() throws CloneNotSupportedException {
        AddRequest request = (AddRequest)super.clone();

        request.dn = dn;
        request.attributes = (Attributes)attributes.clone();

        return request;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("dn: ");
        sb.append(dn);
        sb.append("\n");

        sb.append("changetype: add\n");

        sb.append(attributes);

        return sb.toString();
    }
}
