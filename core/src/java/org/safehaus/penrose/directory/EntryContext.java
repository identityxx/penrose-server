package org.safehaus.penrose.directory;

/**
 * @author Endi Sukma Dewata
 */
public class EntryContext {

    private Directory directory;
    private Entry parent;

    public EntryContext() {
    }

    public Directory getDirectory() {
        return directory;
    }

    public void setDirectory(Directory directory) {
        this.directory = directory;
    }

    public Entry getParent() {
        return parent;
    }

    public void setParent(Entry parent) {
        this.parent = parent;
    }
}
