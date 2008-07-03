package org.safehaus.penrose.ldap;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class ModifyRequest extends Request implements Cloneable {

    protected DN dn;
    protected Collection<Modification> modifications = new ArrayList<Modification>();

    public ModifyRequest() {
    }

    public ModifyRequest(ModifyRequest request) {
        super(request);
        dn = request.getDn();
        modifications.addAll(request.modifications);
    }

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

    public Collection<Modification> getModifications() {
        return modifications;
    }

    public void addModification(Modification modification) {
        modifications.add(modification);
    }

    public void setModifications(Collection<Modification> modifications) {
        if (this.modifications == modifications) return;
        this.modifications.clear();
        if (modifications != null) this.modifications.addAll(modifications);
    }

    public Object clone() throws CloneNotSupportedException {
        ModifyRequest request = (ModifyRequest)super.clone();

        request.dn = dn;

        request.modifications = new ArrayList<Modification>();
        
        for (Modification modification : modifications) {
            request.modifications.add((Modification)modification.clone());
        }

        return request;
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("dn: ");
        sb.append(dn);
        sb.append("\n");

        sb.append("changetype: modify\n");

        for (Modification modification : modifications) {
            Attribute attr = modification.getAttribute();

            sb.append(LDAP.getModificationOperation(modification.getType()));
            sb.append(": ");
            sb.append(attr.getName());
            sb.append("\n");

            sb.append(attr);

            sb.append("-");
            sb.append("\n");
        }

        return sb.toString();
    }
}
