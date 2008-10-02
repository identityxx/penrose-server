package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldapbackend.Attributes;
import org.safehaus.penrose.ldapbackend.DN;

import java.util.ArrayList;
import java.util.Collection;

import org.safehaus.penrose.ldapbackend.Control;
import org.safehaus.penrose.ldapbackend.*;

/**
 * @author Endi S. Dewata
 */
public class PenroseSearchResult implements SearchResult {

    DN dn;
    Attributes attributes;
    Collection<Control> controls = new ArrayList<Control>();

    public PenroseSearchResult(DN dn, Attributes attributes, Collection<Control> controls) {
        this.dn = dn;
        this.attributes = attributes;
        this.controls.addAll(controls);
    }

    public DN getDn() throws Exception {
        return dn;
    }

    public Attributes getAttributes() throws Exception {
        return attributes;
    }

    public Collection<Control> getControls() {
        return controls;
    }

    public void setControls(Collection<Control> controls) {
        if (this.controls == controls) return;
        this.controls.clear();
        if (controls != null) this.controls.addAll(controls);
    }
}
