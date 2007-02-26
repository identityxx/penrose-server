package org.safehaus.penrose.backend;

import com.identyx.javabackend.Attributes;
import com.identyx.javabackend.DN;

import javax.naming.directory.SearchResult;

/**
 * @author Endi S. Dewata
 */
public class PenroseEntry implements com.identyx.javabackend.Entry {

    public SearchResult entry;

    public PenroseEntry(SearchResult entry) {
        this.entry = entry;
    }

    public DN getDn() throws Exception {
        return new PenroseDN(entry.getName());
    }

    public Attributes getAttributes() throws Exception {
        return new PenroseAttributes(entry.getAttributes());
    }
}
