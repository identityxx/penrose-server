package org.safehaus.penrose.partition;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.ldap.*;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface PartitionServiceMBean {

    public void start() throws Exception;
    public void stop() throws Exception;
    public String getStatus() throws Exception;

    public PartitionConfig getPartitionConfig() throws Exception;

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
