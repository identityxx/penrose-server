package org.safehaus.penrose.backend;

import org.safehaus.penrose.entry.Entry;
import com.identyx.javabackend.Attributes;
import com.identyx.javabackend.DN;

/**
 * @author Endi S. Dewata
 */
public class PenroseEntry implements com.identyx.javabackend.Entry {

    public Entry entry;

    public PenroseEntry(Entry entry) {
        this.entry = entry;
    }

    public DN getDn() throws Exception {
        return new PenroseDN(entry.getDn());
    }

    public Attributes getAttributes() throws Exception {
        return new PenroseAttributes(entry.getAttributes());
    }
}
