package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldapbackend.Control;
import org.safehaus.penrose.ldapbackend.DN;
import org.safehaus.penrose.ldapbackend.SearchReference;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class PenroseSearchReference implements SearchReference {

    DN dn;
    Collection<String> urls = new ArrayList<String>();
    Collection<Control> controls = new ArrayList<Control>();

    public PenroseSearchReference(DN dn, Collection<String> urls, Collection<Control> controls) {
        this.dn = dn;
        this.urls.addAll(urls);
        this.controls.addAll(controls);
    }

    public DN getDn() throws Exception {
        return dn;
    }

    public void setDn(DN dn) {
        this.dn = dn;
    }

    public Collection<String> getUrls() throws Exception {
        return urls;
    }

    public void setUrls(Collection<String> urls) {
        if (this.urls == urls) return;
        this.urls.clear();
        if (urls != null) this.urls.addAll(urls);
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