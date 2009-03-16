package org.safehaus.penrose.directory.event;

/**
 * @author Endi Sukma Dewata
 */
public interface DirectoryListener {

    public void entryAdded(DirectoryEvent event) throws Exception;
    public void entryRemoved(DirectoryEvent event) throws Exception;
}
