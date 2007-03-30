package org.safehaus.penrose.ldap;

import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.control.Control;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class SearchResult {

    private Entry entry;
    private Collection<Control> controls = new ArrayList<Control>();

    public SearchResult(Entry entry) {
        this.entry = entry;
    }

    public SearchResult(Entry entry, Collection<Control> controls) {
        this.entry = entry;
        this.controls.addAll(controls);
    }
    
    public Entry getEntry() {
        return entry;
    }

    public void setEntry(Entry entry) {
        this.entry = entry;
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
