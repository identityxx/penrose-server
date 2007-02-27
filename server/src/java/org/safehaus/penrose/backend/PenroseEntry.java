package org.safehaus.penrose.backend;

import javax.naming.directory.SearchResult;

import org.safehaus.penrose.entry.DN;

/**
 * @author Endi S. Dewata
 */
public class PenroseEntry implements com.identyx.javabackend.Entry {

    public SearchResult entry;

    public PenroseEntry(SearchResult entry) {
        this.entry = entry;
    }

    public com.identyx.javabackend.DN getDn() throws Exception {
        return new PenroseDN(new DN(entry.getName()));
    }

    public com.identyx.javabackend.Attributes getAttributes() throws Exception {
        return new PenroseAttributes(entry.getAttributes());
    }
}
