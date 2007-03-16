package org.safehaus.penrose.session;

import org.safehaus.penrose.entry.Entry;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class SearchResult {

    private Entry entry;
    private Collection controls = new ArrayList();

    public SearchResult(Entry entry) {
        this.entry = entry;
    }

    public SearchResult(Entry entry, Collection controls) {
        this.entry = entry;
        this.controls.addAll(controls);
    }
    
    public Entry getEntry() {
        return entry;
    }

    public void setEntry(Entry entry) {
        this.entry = entry;
    }

    public Collection getControls() {
        return controls;
    }

    public void setControls(Collection controls) {
        if (this.controls == controls) return;
        this.controls.clear();
        if (controls != null) this.controls.addAll(controls);
    }
}
