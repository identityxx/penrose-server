package org.safehaus.penrose.backend;

import com.identyx.javabackend.SearchResult;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class PenroseSearchResult implements SearchResult {

    PenroseEntry entry;
    Collection controls = new ArrayList();

    public PenroseSearchResult(PenroseEntry entry, Collection controls) {
        this.entry = entry;
        this.controls.addAll(controls);
    }

    public PenroseEntry getEntry() {
        return entry;
    }

    public void setEntry(PenroseEntry entry) {
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
