package org.safehaus.penrose.backend;

import com.identyx.javabackend.Control;
import com.identyx.javabackend.SearchResult;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class PenroseSearchResult implements SearchResult {

    PenroseEntry entry;
    Collection<Control> controls = new ArrayList<Control>();

    public PenroseSearchResult(PenroseEntry entry, Collection<Control> controls) {
        this.entry = entry;
        this.controls.addAll(controls);
    }

    public PenroseEntry getEntry() {
        return entry;
    }

    public void setEntry(PenroseEntry entry) {
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
