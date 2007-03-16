package org.safehaus.penrose.session;

import org.safehaus.penrose.entry.DN;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class ModifyRequest extends Request {

    protected DN dn;
    protected Collection modifications = new ArrayList();

    public DN getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = new DN(dn);
    }
    
    public void setDn(DN dn) {
        this.dn = dn;
    }

    public Collection getModifications() {
        return modifications;
    }

    public void setModifications(Collection modifications) {
        if (this.modifications == modifications) return;
        this.modifications.clear();
        if (modifications == null) return;
        this.modifications.addAll(modifications);
    }
}
