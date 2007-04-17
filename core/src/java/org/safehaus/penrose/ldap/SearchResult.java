package org.safehaus.penrose.ldap;

import org.safehaus.penrose.control.Control;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.entry.SourceValues;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class SearchResult {

    private EntryMapping entryMapping;

    private DN dn;
    private Attributes attributes = new Attributes();

    private SourceValues sourceValues = new SourceValues();

    private Collection<Control> controls = new ArrayList<Control>();

    public SearchResult(String dn, Attributes attributes) {
        this.dn = new DN(dn);
        this.attributes.add(attributes);
    }

    public SearchResult(DN dn, Attributes attributes) {
        this.dn = dn;
        this.attributes.add(attributes);
    }

    public SearchResult(DN dn, Attributes attributes, Collection<Control> controls) {
        this.dn = dn;
        this.attributes.add(attributes);
        this.controls.addAll(controls);
    }

    public DN getDn() {
        return dn;
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

    public Collection<Control> getControls() {
        return controls;
    }

    public void setControls(Collection<Control> controls) {
        if (this.controls == controls) return;
        this.controls.clear();
        if (controls != null) this.controls.addAll(controls);
    }

    public EntryMapping getEntryMapping() {
        return entryMapping;
    }

    public void setEntryMapping(EntryMapping entryMapping) {
        this.entryMapping = entryMapping;
    }

    public SourceValues getSourceValues() {
        return sourceValues;
    }

    public void setSourceValues(SourceValues sourceValues) {
        this.sourceValues = sourceValues;
    }
}
