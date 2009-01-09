package org.safehaus.penrose.directory;

import org.safehaus.penrose.directory.DirectoryConfig;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.ldap.DN;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface DirectoryServiceMBean {

    public DirectoryConfig getDirectoryConfig() throws Exception;

    public DN getSuffix() throws Exception;
    public Collection<DN> getSuffixes() throws Exception;
    public Collection<String> getRootEntryIds() throws Exception;
    public Collection<String> getEntryIds() throws Exception;

    public String createEntry(EntryConfig entryConfig) throws Exception;
    public void updateEntry(EntryConfig entryConfig) throws Exception;
    public void removeEntry(String id) throws Exception;
}