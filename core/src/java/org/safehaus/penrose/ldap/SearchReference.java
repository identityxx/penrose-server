package org.safehaus.penrose.ldap;

import org.safehaus.penrose.control.Control;

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class SearchReference implements Serializable, Cloneable {

    private DN dn;
    private Collection<String> urls = new ArrayList<String>();
    private Collection<Control> controls = new ArrayList<Control>();

    public SearchReference() {
    }

    public SearchReference(String dn, Collection<String> urls) {
        this.dn = new DN(dn);
        this.urls.addAll(urls);
    }

    public SearchReference(DN dn, Collection<String> urls) {
        this.dn = dn;
        this.urls.addAll(urls);
    }

    public SearchReference(DN dn, Collection<String> urls, Collection<Control> controls) {
        this.dn = dn;
        this.urls.addAll(urls);
        this.controls.addAll(controls);
    }

    public DN getDn() {
        return dn;
    }

    public void setDn(DN dn) {
        this.dn = dn;
    }

    public Collection<String> getUrls() {
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

    public Object clone() throws CloneNotSupportedException {
        SearchReference sr = (SearchReference)super.clone();

        sr.dn = dn;

        sr.urls = new ArrayList<String>();
        sr.urls.addAll(urls);

        sr.controls = new ArrayList<Control>();
        for (Control control : controls) {
            sr.controls.add((Control)control.clone());
        }

        return sr;
    }
}