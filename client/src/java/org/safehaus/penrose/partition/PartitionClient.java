package org.safehaus.penrose.partition;

import org.safehaus.penrose.directory.DirectoryClient;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.client.BaseClient;
import org.safehaus.penrose.client.PenroseClient;
import org.safehaus.penrose.connection.ConnectionManagerClient;
import org.safehaus.penrose.mapping.MappingClient;
import org.safehaus.penrose.scheduler.SchedulerClient;
import org.safehaus.penrose.mapping.MappingManagerClient;
import org.safehaus.penrose.module.ModuleManagerClient;
import org.safehaus.penrose.source.SourceManagerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionClient extends BaseClient implements PartitionServiceMBean {

    public static Logger log = LoggerFactory.getLogger(PartitionClient.class);

    public PartitionClient(PenroseClient client, String name) throws Exception {
        super(client, name, getStringObjectName(name));
    }

    public static String getStringObjectName(String name) {
        return "Penrose:type=partition,name="+name;
    }

    public void start() throws Exception {
        invoke(
                "start",
                new Object[] { },
                new String[] { });
    }

    public void stop() throws Exception {
        invoke(
                "stop",
                new Object[] { },
                new String[] { });
    }

    public String getStatus() throws Exception {
        return (String)getAttribute("Status");
    }

    public PartitionConfig getPartitionConfig() throws Exception {
        return (PartitionConfig)getAttribute("PartitionConfig");
    }

    public void store() throws Exception {
        invoke(
                "store",
                new Object[] { },
                new String[] { }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Connections
    ////////////////////////////////////////////////////////////////////////////////

    public ConnectionManagerClient getConnectionManagerClient() throws Exception {
        return new ConnectionManagerClient(client, name);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Sources
    ////////////////////////////////////////////////////////////////////////////////

    public SourceManagerClient getSourceManagerClient() throws Exception {
        return new SourceManagerClient(client, name);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Mappings
    ////////////////////////////////////////////////////////////////////////////////

    public MappingManagerClient getMappingManagerClient() throws Exception {
        return new MappingManagerClient(client, name);
    }

    public MappingClient getMappingClient(String mappingName) throws Exception {
        return new MappingClient(client, name, mappingName);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Directory
    ////////////////////////////////////////////////////////////////////////////////

    public DirectoryClient getDirectoryClient() throws Exception {
        return new DirectoryClient(client, name);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Modules
    ////////////////////////////////////////////////////////////////////////////////

    public ModuleManagerClient getModuleManagerClient() throws Exception {
        return new ModuleManagerClient(client, name);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Scheduler
    ////////////////////////////////////////////////////////////////////////////////

    public SchedulerClient getSchedulerClient() throws Exception {
        return new SchedulerClient(client, name);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public AddResponse add(
            String dn,
            Attributes attributes
    ) throws Exception {
        return (AddResponse)invoke(
                "add",
                new Object[] { dn, attributes },
                new String[] { String.class.getName(), Attributes.class.getName() }
        );
    }

    public AddResponse add(
            RDN rdn,
            Attributes attributes
    ) throws Exception {
        return (AddResponse)invoke(
                "add",
                new Object[] { rdn, attributes },
                new String[] { RDN.class.getName(), Attributes.class.getName() }
        );
    }

    public AddResponse add(
            DN dn,
            Attributes attributes
    ) throws Exception {
        return (AddResponse)invoke(
                "add",
                new Object[] { dn, attributes },
                new String[] { DN.class.getName(), Attributes.class.getName() }
        );
    }

    public AddResponse add(
            AddRequest request,
            AddResponse response
    ) throws Exception {
        AddResponse newResponse = (AddResponse)invoke(
                "add",
                new Object[] { request, response },
                new String[] { AddRequest.class.getName(), AddResponse.class.getName() }
        );
        response.copy(newResponse);
        return response;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public DeleteResponse delete(
            String dn
    ) throws Exception {
        return (DeleteResponse)invoke(
                "delete",
                new Object[] { dn },
                new String[] { String.class.getName() }
        );
    }

    public DeleteResponse delete(
            RDN rdn
    ) throws Exception {
        return (DeleteResponse)invoke(
                "delete",
                new Object[] { rdn },
                new String[] { RDN.class.getName() }
        );
    }

    public DeleteResponse delete(
            DN dn
    ) throws Exception {
        return (DeleteResponse)invoke(
                "delete",
                new Object[] { dn },
                new String[] { DN.class.getName() }
        );
    }

    public DeleteResponse delete(
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {
        DeleteResponse newResponse = (DeleteResponse)invoke(
                "delete",
                new Object[] { request, response },
                new String[] { DeleteRequest.class.getName(), DeleteResponse.class.getName() }
        );
        response.copy(newResponse);
        return response;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResult find(
            String dn
    ) throws Exception {
        return (SearchResult)invoke(
                "find",
                new Object[] { dn },
                new String[] { String.class.getName() }
        );
    }

    public SearchResult find(
            RDN rdn
    ) throws Exception {
        return (SearchResult)invoke(
                "find",
                new Object[] { rdn },
                new String[] { RDN.class.getName() }
        );
    }

    public SearchResult find(
            DN dn
    ) throws Exception {
        return (SearchResult)invoke(
                "find",
                new Object[] { dn },
                new String[] { DN.class.getName() }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ModifyResponse modify(
            String dn,
            Collection<Modification> modifications
    ) throws Exception {
        return (ModifyResponse)invoke(
                "modify",
                new Object[] { dn, modifications },
                new String[] { String.class.getName(), Collection.class.getName() }
        );
    }

    public ModifyResponse modify(
            RDN rdn,
            Collection<Modification> modifications
    ) throws Exception {
        return (ModifyResponse)invoke(
                "modify",
                new Object[] { rdn, modifications },
                new String[] { RDN.class.getName(), Collection.class.getName() }
        );
    }

    public ModifyResponse modify(
            DN dn,
            Collection<Modification> modifications
    ) throws Exception {
        return (ModifyResponse)invoke(
                "modify",
                new Object[] { dn, modifications },
                new String[] { DN.class.getName(), Collection.class.getName() }
        );
    }

    public ModifyResponse modify(
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {
        ModifyResponse newResponse = (ModifyResponse)invoke(
                "modify",
                new Object[] { request, response },
                new String[] { ModifyRequest.class.getName(), ModifyResponse.class.getName() }
        );
        response.copy(newResponse);
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
        return (ModRdnResponse)invoke(
                "modrdn",
                new Object[] { dn, newRdn, deleteOldRdn },
                new String[] { String.class.getName(), String.class.getName(), boolean.class.getName() }
        );
    }

    public ModRdnResponse modrdn(
            RDN rdn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {
        return (ModRdnResponse)invoke(
                "modrdn",
                new Object[] { rdn, newRdn, deleteOldRdn },
                new String[] { RDN.class.getName(), RDN.class.getName(), boolean.class.getName() }
        );
    }

    public ModRdnResponse modrdn(
            DN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {
        return (ModRdnResponse)invoke(
                "modrdn",
                new Object[] { dn, newRdn, deleteOldRdn },
                new String[] { DN.class.getName(), RDN.class.getName(), boolean.class.getName() }
        );
    }

    public ModRdnResponse modrdn(
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {
        ModRdnResponse newResponse = (ModRdnResponse)invoke(
                "modrdn",
                new Object[] { request, response },
                new String[] { ModRdnRequest.class.getName(), ModRdnResponse.class.getName() }
        );
        response.copy(newResponse);
        return response;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResponse search(
            String dn,
            String filter,
            Integer scope
    ) throws Exception {
        return (SearchResponse)invoke(
                "search",
                new Object[] { dn, filter, scope },
                new String[] { String.class.getName(), String.class.getName(), Integer.class.getName() }
        );
    }

    public SearchResponse search(
            RDN rdn,
            Filter filter,
            Integer scope
    ) throws Exception {
        return (SearchResponse)invoke(
                "search",
                new Object[] { rdn, filter, scope },
                new String[] { RDN.class.getName(), Filter.class.getName(), Integer.class.getName() }
        );
    }

    public SearchResponse search(
            DN dn,
            Filter filter,
            Integer scope
    ) throws Exception {
        return (SearchResponse)invoke(
                "search",
                new Object[] { dn, filter, scope },
                new String[] { DN.class.getName(), Filter.class.getName(), Integer.class.getName() }
        );
    }

    public SearchResponse search(
            SearchRequest request,
            SearchResponse response
    ) throws Exception {
        SearchResponse newResponse = (SearchResponse)invoke(
                "search",
                new Object[] { request, response },
                new String[] { SearchRequest.class.getName(), SearchResponse.class.getName() }
        );
        response.copy(newResponse);
        return response;
    }
}
