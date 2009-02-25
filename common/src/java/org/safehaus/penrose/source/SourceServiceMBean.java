package org.safehaus.penrose.source;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.filter.Filter;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface SourceServiceMBean {

    public final static String STARTED = "STARTED";
    public final static String STOPPED = "STOPPED";

    public String getStatus() throws Exception;
    public Long getCount() throws Exception;

    public void create() throws Exception;
    public void clear() throws Exception;
    public void drop() throws Exception;

    public String getAdapterName() throws Exception;
    public String getConnectionName() throws Exception;

    public Collection<String> getFieldNames() throws Exception;
    public Collection<FieldConfig> getFieldConfigs() throws Exception;

    public SourceConfig getSourceConfig() throws Exception;
    public void setSourceConfig(SourceConfig sourceConfig) throws Exception;

    public String getParameter(String name) throws Exception;
    public Collection<String> getParameterNames() throws Exception;

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

    public SearchResponse search(String dn, String filter, int scope) throws Exception;
    public SearchResponse search(RDN rdn, Filter filter, int scope) throws Exception;
    public SearchResponse search(DN dn, Filter filter, int scope) throws Exception;
    public SearchResponse search(SearchRequest request, SearchResponse response) throws Exception;
}
