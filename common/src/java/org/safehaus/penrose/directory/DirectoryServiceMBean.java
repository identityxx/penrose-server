package org.safehaus.penrose.directory;

import org.safehaus.penrose.directory.DirectoryConfig;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.ldap.DN;

import java.util.Collection;
import java.util.List;

/**
 * @author Endi Sukma Dewata
 */
public interface DirectoryServiceMBean {

    public DirectoryConfig getDirectoryConfig() throws Exception;

    public DN getSuffix() throws Exception;
    public Collection<DN> getSuffixes() throws Exception;
    public Collection<String> getRootEntryNames() throws Exception;
    public Collection<String> getEntryNames() throws Exception;

    public String getParentName(String entryName) throws Exception;
    public List<String> getChildNames(String entryName) throws Exception;
    public void setChildNames(String entryName, List<String> childNames) throws Exception;

    public String getEntryName(DN dn) throws Exception;
    public DN getEntryDn(String entryName) throws Exception;
    public EntryConfig getEntryConfig(String entryName) throws Exception;

    public String createEntry(EntryConfig entryConfig) throws Exception;
    public void updateEntry(String entryName, EntryConfig entryConfig) throws Exception;
    public void removeEntry(String entryName) throws Exception;
}