package org.safehaus.penrose.partition;

import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.directory.EntryClient;
import org.safehaus.penrose.directory.DirectoryClient;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.management.BaseClient;
import org.safehaus.penrose.management.PenroseClient;
import org.safehaus.penrose.management.partition.PartitionServiceMBean;
import org.safehaus.penrose.connection.ConnectionClient;
import org.safehaus.penrose.mapping.MappingClient;
import org.safehaus.penrose.module.ModuleClient;
import org.safehaus.penrose.scheduler.SchedulerClient;
import org.safehaus.penrose.source.SourceClient;
import org.safehaus.penrose.mapping.MappingConfig;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleMapping;
import org.safehaus.penrose.source.SourceConfig;
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

    public Collection<String> getConnectionNames() throws Exception {
        return (Collection<String>)getAttribute("ConnectionNames");
    }

    public ConnectionClient getConnectionClient(String connectionName) throws Exception {
        return new ConnectionClient(client, name, connectionName);
    }

    public void createConnection(ConnectionConfig connectionConfig) throws Exception {
        invoke(
                "createConnection",
                new Object[] { connectionConfig },
                new String[] { ConnectionConfig.class.getName() }
        );
    }

    public void updateConnection(String name, ConnectionConfig connectionConfig) throws Exception {
        invoke(
                "updateConnection",
                new Object[] { name, connectionConfig },
                new String[] { String.class.getName(), ConnectionConfig.class.getName() }
        );
    }

    public void removeConnection(String name) throws Exception {
        invoke(
                "removeConnection",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Sources
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getSourceNames() throws Exception {
        return (Collection<String>)getAttribute("SourceNames");
    }

    public SourceClient getSourceClient(String sourceName) throws Exception {
        return new SourceClient(client, name, sourceName);
    }

    public void createSource(SourceConfig sourceConfig) throws Exception {
        invoke(
                "createSource",
                new Object[] { sourceConfig },
                new String[] { SourceConfig.class.getName() }
        );
    }

    public void updateSource(String name, SourceConfig sourceConfig) throws Exception {
        invoke(
                "updateSource",
                new Object[] { name, sourceConfig },
                new String[] { String.class.getName(), SourceConfig.class.getName() }
        );
    }

    public void removeSource(String name) throws Exception {
        invoke(
                "removeSource",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Mappings
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getMappingNames() throws Exception {
        return (Collection<String>)getAttribute("MappingNames");
    }

    public MappingClient getMappingClient(String mappingName) throws Exception {
        return new MappingClient(client, name, mappingName);
    }

    public void createMapping(MappingConfig mappingConfig) throws Exception {
        invoke(
                "createMapping",
                new Object[] { mappingConfig },
                new String[] { MappingConfig.class.getName() }
        );
    }

    public void updateMapping(String mappingName, MappingConfig mappingConfig) throws Exception {
        invoke(
                "updateMapping",
                new Object[] { mappingName, mappingConfig },
                new String[] { String.class.getName(), MappingConfig.class.getName() }
        );
    }

    public void removeMapping(String mappingName) throws Exception {
        invoke(
                "removeMapping",
                new Object[] { mappingName },
                new String[] { String.class.getName() }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Directory
    ////////////////////////////////////////////////////////////////////////////////

    public DirectoryClient getDirectoryClient() throws Exception {
        return new DirectoryClient(client, name);
    }

    public Collection<DN> getSuffixes() throws Exception {
        return (Collection<DN>)getAttribute("Suffixes");
    }

    public Collection<String> getRootEntryIds() throws Exception {
        return (Collection<String>)getAttribute("RootEntryIds");
    }

    public Collection<String> getEntryIds() throws Exception {
        return (Collection<String>)getAttribute("EntryIds");
    }

    public EntryClient getEntryClient(String entryId) throws Exception {
        return new EntryClient(client, name, entryId);
    }

    public String createEntry(EntryConfig entryConfig) throws Exception {
        return (String)invoke(
                "createEntry",
                new Object[] { entryConfig },
                new String[] { EntryConfig.class.getName() }
        );
    }

    public void updateEntry(String id, EntryConfig entryConfig) throws Exception {
        invoke(
                "updateEntry",
                new Object[] { id, entryConfig },
                new String[] { String.class.getName(), EntryConfig.class.getName() }
        );
    }

    public void removeEntry(String id) throws Exception {
        invoke(
                "removeEntry",
                new Object[] { id },
                new String[] { String.class.getName() }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Modules
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getModuleNames() throws Exception {
        return (Collection<String>)getAttribute("ModuleNames");
    }

    public ModuleClient getModuleClient(String moduleName) throws Exception {
        return new ModuleClient(client, name, moduleName);
    }

    public void createModule(ModuleConfig moduleConfig) throws Exception {
        invoke(
                "createModule",
                new Object[] { moduleConfig },
                new String[] { ModuleConfig.class.getName() }
        );
    }

    public void createModule(ModuleConfig moduleConfig, Collection<ModuleMapping> moduleMappings) throws Exception {
        invoke(
                "createModule",
                new Object[] { moduleConfig, moduleMappings },
                new String[] { ModuleConfig.class.getName(), Collection.class.getName() }
        );
    }

    public void updateModule(String name, ModuleConfig moduleConfig) throws Exception {
        invoke(
                "updateModule",
                new Object[] { name, moduleConfig },
                new String[] { String.class.getName(), ModuleConfig.class.getName() }
        );
    }

    public void removeModule(String name) throws Exception {
        invoke(
                "removeModule",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
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
