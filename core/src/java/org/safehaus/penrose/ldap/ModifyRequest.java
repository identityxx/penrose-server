package org.safehaus.penrose.ldap;

import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.RDN;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class ModifyRequest extends Request {

    protected DN dn;
    protected Collection<Modification> modifications = new ArrayList<Modification>();

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
}
