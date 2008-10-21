package org.safehaus.penrose.partition;

import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.mapping.MappingConfig;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleMapping;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.source.SourceConfig;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface PartitionServiceMBean {

    public void start() throws Exception;
    public void stop() throws Exception;
    public String getStatus() throws Exception;

    public PartitionConfig getPartitionConfig() throws Exception;

    public DN getSuffix() throws Exception;
    public Collection<DN> getSuffixes() throws Exception;
    public Collection<String> getRootEntryIds() throws Exception;

    public Collection<String> getEntryIds() throws Exception;
    public String createEntry(EntryConfig entryConfig) throws Exception;
    public void updateEntry(String id, EntryConfig entryConfig) throws Exception;
    public void removeEntry(String id) throws Exception;

    public Collection<String> getConnectionNames() throws Exception;
    public void createConnection(ConnectionConfig connectionConfig) throws Exception;
    public void updateConnection(String name, ConnectionConfig connectionConfig) throws Exception;
    public void removeConnection(String name) throws Exception;

    public Collection<String> getSourceNames() throws Exception;
    public void createSource(SourceConfig sourceConfig) throws Exception;
    public void updateSource(String name, SourceConfig sourceConfig) throws Exception;
    public void removeSource(String name) throws Exception;

    public Collection<String> getMappingNames() throws Exception;
    public void createMapping(MappingConfig mappingConfig) throws Exception;
    public void updateMapping(String name, MappingConfig connectionConfig) throws Exception;
    public void removeMapping(String name) throws Exception;

    public Collection<String> getModuleNames() throws Exception;
    public void createModule(ModuleConfig moduleConfig) throws Exception;
    public void createModule(ModuleConfig moduleConfig, Collection<ModuleMapping> moduleMappings) throws Exception;
    public void updateModule(String name, ModuleConfig moduleConfig) throws Exception;
    public void removeModule(String name) throws Exception;

    public void store() throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public AddResponse add(String dn, Attributes attributes) throws Exception;
    public AddResponse add(RDN rdn, Attributes attributes) throws Exception;
    public AddResponse add(DN dn, Attributes attributes) throws Exception;
    public AddResponse add(AddRequest request, AddResponse response) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public DeleteResponse delete(String dn) throws Exception;
    public DeleteResponse delete(RDN rdn) throws Exception;
    public DeleteResponse delete(DN dn) throws Exception;
    public DeleteResponse delete(DeleteRequest request, DeleteResponse response) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResult find(String dn) throws Exception;
    public SearchResult find(RDN rdn) throws Exception;
    public SearchResult find(DN dn) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ModifyResponse modify(String dn, Collection<Modification> modifications) throws Exception;
    public ModifyResponse modify(RDN rdn, Collection<Modification> modifications) throws Exception;
    public ModifyResponse modify(DN dn, Collection<Modification> modifications) throws Exception;
    public ModifyResponse modify(ModifyRequest request, ModifyResponse response) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ModRdnResponse modrdn(String dn, String newRdn, boolean deleteOldRdn) throws Exception;
    public ModRdnResponse modrdn(RDN rdn, RDN newRdn, boolean deleteOldRdn) throws Exception;
    public ModRdnResponse modrdn(DN dn, RDN newRdn, boolean deleteOldRdn) throws Exception;
    public ModRdnResponse modrdn(ModRdnRequest request, ModRdnResponse response) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResponse search(String dn, String filter, Integer scope) throws Exception;
    public SearchResponse search(RDN rdn, Filter filter, Integer scope) throws Exception;
    public SearchResponse search(DN dn, Filter filter, Integer scope) throws Exception;
    public SearchResponse search(SearchRequest request, SearchResponse response) throws Exception;
}
