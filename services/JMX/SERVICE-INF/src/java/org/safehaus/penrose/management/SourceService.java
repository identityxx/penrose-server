package org.safehaus.penrose.management;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class SourceService extends JMXService implements SourceServiceMBean {

    private Source source;

    public SourceService(Source source) throws Exception {
        super(source, source.getDescription());
        
        this.source = source;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public Long getCount() throws Exception {
        return source.getCount();
    }

    public void create() throws Exception {
        Partition partition = source.getPartition();
        log.debug("Creating source "+partition.getName()+"/"+source.getName()+"...");
        source.create();
        log.debug("Source created.");
    }

    public void clear() throws Exception {
        Partition partition = source.getPartition();
        log.debug("Clearing source "+partition.getName()+"/"+source.getName()+"...");
        source.clear();
        log.debug("Source cleared.");
    }

    public void drop() throws Exception {
        Partition partition = source.getPartition();
        log.debug("Dropping source "+partition.getName()+"/"+source.getName()+"...");
        source.drop();
        log.debug("Source dropped.");
    }

    public SourceConfig getSourceConfig() throws Exception {
        return source.getSourceConfig();
    }

    public String getConnectionName() throws Exception {
        return source.getConnectionName();
    }

    public String getObjectName() {
        Partition partition = source.getPartition();
        return SourceClient.getObjectName(partition.getName(), source.getName());
    }

    public String getParameter(String name) {
        return source.getParameter(name);
    }

    public Collection<String> getParameterNames() {
        return source.getParameterNames();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public AddResponse add(
            String dn,
            Attributes attributes
    ) throws Exception {

        AddResponse response = source.add(dn, attributes);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    public AddResponse add(
            RDN rdn,
            Attributes attributes
    ) throws Exception {

        AddResponse response = source.add(rdn, attributes);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    public AddResponse add(
            DN dn,
            Attributes attributes
    ) throws Exception {

        AddResponse response = source.add(dn, attributes);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    public AddResponse add(
            AddRequest request,
            AddResponse response
    ) throws Exception {

        source.add(request, response);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public DeleteResponse delete(
            String dn
    ) throws Exception {

        DeleteResponse response = source.delete(dn);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    public DeleteResponse delete(
            RDN rdn
    ) throws Exception {

        DeleteResponse response = source.delete(rdn);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    public DeleteResponse delete(
            DN dn
    ) throws Exception {

        DeleteResponse response = source.delete(dn);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    public DeleteResponse delete(
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        source.delete(request, response);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResult find(String dn) throws Exception {

        return source.find(dn);
    }

    public SearchResult find(RDN rdn) throws Exception {

        return source.find(rdn);
    }

    public SearchResult find(
            DN dn
    ) throws Exception {

        return source.find(dn);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ModifyResponse modify(
            String dn,
            Collection<Modification> modifications
    ) throws Exception {

        ModifyResponse response = source.modify(dn, modifications);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    public ModifyResponse modify(
            RDN rdn,
            Collection<Modification> modifications
    ) throws Exception {

        ModifyResponse response = source.modify(rdn, modifications);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    public ModifyResponse modify(
            DN dn,
            Collection<Modification> modifications
    ) throws Exception {

        ModifyResponse response = source.modify(dn, modifications);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    public ModifyResponse modify(
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        source.modify(request, response);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ModRdnResponse modrdn(
            String dn,
            String newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        ModRdnResponse response = source.modrdn(dn, newRdn, deleteOldRdn);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    public ModRdnResponse modrdn(
            RDN rdn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        ModRdnResponse response = source.modrdn(rdn, newRdn, deleteOldRdn);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    public ModRdnResponse modrdn(
            DN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        ModRdnResponse response = source.modrdn(dn, newRdn, deleteOldRdn);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    public ModRdnResponse modrdn(
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        source.modrdn(request, response);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResponse search(
            String dn,
            String filter,
            int scope
    ) throws Exception {

        SearchResponse response = source.search(dn, filter, scope);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    public SearchResponse search(
            RDN rdn,
            Filter filter,
            int scope
    ) throws Exception {

        SearchResponse response = source.search(rdn, filter, scope);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    public SearchResponse search(
            DN dn,
            Filter filter,
            int scope
    ) throws Exception {

        SearchResponse response = source.search(dn, filter, scope);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }

    public SearchResponse search(
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        source.search(request, response);

        int rc = response.getReturnCode();
        log.debug("RC: "+rc);

        return response;
    }
}
