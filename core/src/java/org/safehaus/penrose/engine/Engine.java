/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.matchingRule.EqualityMatchingRule;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connector.ConnectorManager;
import org.safehaus.penrose.partition.PartitionConfigs;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.adapter.Adapter;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public abstract class Engine {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    protected EngineConfig engineConfig;
    protected Partition partition;
    protected PenroseContext penroseContext;

    protected SchemaManager schemaManager;
    protected InterpreterManager interpreterManager;
    protected ConnectorManager connectorManager;
    protected PartitionConfigs partitionConfigs;

    public String getName() {
        return engineConfig.getName();
    }
    
    public Collection<String> getParameterNames() {
        return engineConfig.getParameterNames();
    }

    public String getParameter(String name) {
        return engineConfig.getParameter(name);
    }

    public void init(EngineConfig engineConfig) throws Exception {
        this.engineConfig = engineConfig;
        init();
    }

    public void init() throws Exception {
    }

    public EngineConfig getEngineConfig() {
        return engineConfig;
    }

    public void setEngineConfig(EngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    public InterpreterManager getInterpreterManager() {
        return interpreterManager;
    }

    public void setInterpreterManager(InterpreterManager interpreterManager) {
        this.interpreterManager = interpreterManager;
    }

    public Connector getConnector(SourceRef sourceRef) {
        Source source = sourceRef.getSource();
        return getConnector(source.getSourceConfig());
    }

    public Connector getConnector(SourceConfig sourceConfig) {
        String connectorName = sourceConfig.getParameter("connectorName");
        return connectorManager.getConnector(connectorName);
    }

    public PartitionConfigs getPartitionManager() {
        return partitionConfigs;
    }

    public void setPartitionManager(PartitionConfigs partitionConfigs) throws Exception {
        this.partitionConfigs = partitionConfigs;
    }

    public Attributes createAttributes(
            EntryMapping entryMapping,
            SourceValues sourceValues,
            Interpreter interpreter
            ) throws Exception {

        Attributes attributes = new Attributes();

        if (sourceValues != null) interpreter.set(sourceValues);

        Collection<AttributeMapping> attributeMappings = entryMapping.getAttributeMappings();
        for (AttributeMapping attributeMapping : attributeMappings) {

            Object value = interpreter.eval(attributeMapping);
            if (value == null) continue;

            String name = attributeMapping.getName();
            attributes.addValue(name, value);
        }

        interpreter.clear();

        Collection<String> objectClasses = entryMapping.getObjectClasses();
        for (String objectClass : objectClasses) {
            attributes.addValue("objectClass", objectClass);
        }

        return attributes;
    }

    public String getStartingSourceName(Partition partition, EntryMapping entryMapping) throws Exception {

        log.debug("Searching the starting sourceMapping for "+entryMapping.getDn());
/*
        Collection relationships = entryMapping.getRelationships();
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();

            for (Iterator j=relationship.getOperands().iterator(); j.hasNext(); ) {
                String operand = j.next().toString();

                int index = operand.indexOf(".");
                if (index < 0) continue;

                String sourceName = operand.substring(0, index);
                SourceMapping sourceMapping = entryMapping.getSourceMapping(sourceName);
                SourceMapping effectiveSourceMapping = partition.getEffectiveSourceMapping(entryMapping, sourceName);

                if (sourceMapping == null && effectiveSourceMapping != null) {
                    log.debug("Source "+sourceName+" is defined in parent entry");
                    return sourceName;
                }

            }
        }
*/
        Iterator i = entryMapping.getSourceMappings().iterator();
        if (!i.hasNext()) return null;

        SourceMapping sourceMapping = (SourceMapping)i.next();
        log.debug("Source "+sourceMapping.getName()+" is the first defined in entry");
        return sourceMapping.getName();
    }

    public boolean isStatic(Partition partition, EntryMapping entryMapping) throws Exception {
        PartitionConfig partitionConfig = partition.getPartitionConfig();
        Collection effectiveSources = partitionConfig.getDirectoryConfigs().getEffectiveSourceMappings(entryMapping);
        if (effectiveSources.size() > 0) return false;

        Collection<AttributeMapping> attributeMappings = entryMapping.getAttributeMappings();
        for (AttributeMapping attributeMapping : attributeMappings) {
            if (attributeMapping.getConstant() == null) return false;
        }

        EntryMapping parentMapping = partitionConfig.getDirectoryConfigs().getParent(entryMapping);
        return parentMapping == null || isStatic(partition, parentMapping);

    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract void add(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN dn = request.getDn();
        byte[] password = request.getPassword();

        SearchResult searchResult = find(session, partition, entryMapping, dn);

        Attributes attributes = searchResult.getAttributes();
        Attribute attribute = attributes.get("userPassword");

        if (attribute == null) {
            log.debug("Attribute userPassword not found");
            throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
        }

        Collection<Object> userPasswords = attribute.getValues();
        for (Object userPassword : userPasswords) {
            if (debug) log.debug("userPassword: " + userPassword);
            if (PasswordUtil.comparePassword(password, userPassword)) return;
        }

        throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        DN dn = request.getDn();
        String attributeName = request.getAttributeName();
        Object attributeValue = request.getAttributeValue();

        SearchResult searchResult = find(session, partition, entryMapping, dn);

        Attributes attributes = searchResult.getAttributes();
        Attribute attribute = attributes.get(attributeName);
        if (attribute == null) {
            if (debug) log.debug("Attribute "+attributeName+" not found.");

            response.setReturnCode(LDAP.COMPARE_FALSE);
            return;
        }

        Collection<Object> values = attribute.getValues();
        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        String equality = attributeType == null ? null : attributeType.getEquality();
        EqualityMatchingRule equalityMatchingRule = EqualityMatchingRule.getInstance(equality);

        if (debug) log.debug("Comparing values:");
        for (Object value : values) {
            boolean b = equalityMatchingRule.compare(value, attributeValue);
            if (debug) log.debug(" - [" + value + "] => " + b);

            if (b) {
                response.setReturnCode(LDAP.COMPARE_TRUE);
                return;
            }

        }

        response.setReturnCode(LDAP.COMPARE_FALSE);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract void delete(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResult find(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            DN dn
    ) throws Exception {

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setScope(SearchRequest.SCOPE_BASE);

        SearchResponse response = new SearchResponse();

        search(
                session,
                partition,
                entryMapping,
                request,
                response
        );

        if (!response.hasNext()) {
            if (debug) log.debug("Entry "+dn+" not found");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        return response.next();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract void modify(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract void modrdn(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        search(
                session,
                partition,
                entryMapping,
                entryMapping,
                request,
                response
        );
    }

    public abstract void search(
            Session session,
            Partition partition,
            EntryMapping baseMapping,
            EntryMapping entryMapping,
            SearchRequest request,
            SearchResponse response
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Miscelleanous
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public RDN createFilter(
            Partition partition,
            Interpreter interpreter,
            SourceMapping sourceMapping,
            EntryMapping entryMapping,
            RDN rdn
    ) throws Exception {

        if (sourceMapping == null) {
            return new RDN();
        }

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        Collection<FieldMapping> fields = partitionConfig.getSourceConfigs().getSearchableFields(sourceMapping);

        interpreter.set(rdn);

        RDNBuilder rb = new RDNBuilder();
        for (FieldMapping fieldMapping : fields) {
            String name = fieldMapping.getName();

            Object value = interpreter.eval(fieldMapping);
            if (value == null) continue;

            //log.debug("   ==> "+field.getName()+"="+value);
            //rb.set(source.getName()+"."+name, value);
            rb.set(name, value);
        }

        //if (rb.isEmpty()) return null;

        interpreter.clear();

        return rb.toRdn();
    }

    public Collection<DN> computeDns(
            Partition partition,
            Interpreter interpreter,
            EntryMapping entryMapping
    ) throws Exception {

        log.debug("Computing DN:");

        Collection<DN> dns = new ArrayList<DN>();
        computeDns(partition, interpreter, entryMapping, dns);
        return dns;
    }

    public void computeDns(
            Partition partition,
            Interpreter interpreter,
            EntryMapping entryMapping,
            Collection<DN> dns
    ) throws Exception {

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        EntryMapping parentMapping = partitionConfig.getDirectoryConfigs().getParent(entryMapping);

        Collection<DN> parentDns = new ArrayList<DN>();
        if (parentMapping != null) {
            computeDns(partition, interpreter, parentMapping, parentDns);

        } else if (!entryMapping.getParentDn().isEmpty()) {
            parentDns.add(entryMapping.getParentDn());
        }

        if (parentDns.isEmpty()) {
            DN dn = entryMapping.getDn();
            if (debug) log.debug("DN: "+dn);
            dns.add(dn);

        } else {
            Collection<RDN> rdns = computeRdns(interpreter, entryMapping);

            DNBuilder db = new DNBuilder();

            for (RDN rdn : rdns) {
                //log.info("Processing RDN: "+rdn);

                for (DN parentDn : parentDns) {
                    //log.debug("Appending parent DN: "+parentDn);

                    db.set(rdn);
                    db.append(parentDn);
                    DN dn = db.toDn();

                    if (debug) log.debug("DN: " + dn);
                    dns.add(dn);
                }
            }
        }
    }

    public Collection<RDN> computeRdns(
            Interpreter interpreter,
            EntryMapping entryMapping
    ) throws Exception {

        //log.debug("Computing RDNs:");
        Attributes attributes = new Attributes();

        Collection<AttributeMapping> rdnAttributes = entryMapping.getRdnAttributeMappings();
        for (AttributeMapping attributeMapping : rdnAttributes) {
            String name = attributeMapping.getName();

            Object value = interpreter.eval(attributeMapping);
            if (value == null) continue;

            attributes.addValue(name, value);
        }

        return TransformEngine.convert(attributes);
    }

    public ConnectorManager getConnectorManager() {
        return connectorManager;
    }

    public void setConnectorManager(ConnectorManager connectorManager) {
        this.connectorManager = connectorManager;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;

        schemaManager = penroseContext.getSchemaManager();
        interpreterManager = penroseContext.getInterpreterManager();
        connectorManager = penroseContext.getConnectorManager();
        partitionConfigs = penroseContext.getPartitionConfigs();
    }

    public List<Collection<SourceRef>> getGroupsOfSources(
            Partition partition,
            EntryMapping entryMapping
    ) throws Exception {

        List<Collection<SourceRef>> results = new ArrayList<Collection<SourceRef>>();

        Collection<SourceRef> list = new ArrayList<SourceRef>();
        Connection lastConnection = null;

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        for (EntryMapping em : partitionConfig.getDirectoryConfigs().getPath(entryMapping)) {

            for (SourceRef sourceRef : partition.getSourceRefs(em)) {

                Source source = sourceRef.getSource();
                Connection connection = source.getConnection();
                Adapter adapter = connection.getAdapter();

                if (lastConnection == null) {
                    lastConnection = connection;

                } else if (lastConnection != connection || !adapter.isJoinSupported()) {
                    results.add(list);
                    list = new ArrayList<SourceRef>();
                    lastConnection = connection;
                }

                list.add(sourceRef);
            }
        }

        if (!list.isEmpty()) results.add(list);
        
        return results;
    }

    public List<Collection<SourceRef>> getGroupsOfSources(
            Partition partition,
            EntryMapping baseMapping,
            EntryMapping entryMapping
    ) throws Exception {

        if (entryMapping == baseMapping) {
            return getGroupsOfSources(partition, entryMapping);
        }

        List<Collection<SourceRef>> results = new ArrayList<Collection<SourceRef>>();

        Collection<SourceRef> list = new ArrayList<SourceRef>();
        Connection lastConnection = null;

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        for (EntryMapping em : partitionConfig.getDirectoryConfigs().getRelativePath(baseMapping, entryMapping)) {
            
            for (SourceRef sourceRef : partition.getSourceRefs(em)) {

                Source source = sourceRef.getSource();
                Connection connection = source.getConnection();
                Adapter adapter = connection.getAdapter();

                if (lastConnection == null) {
                    lastConnection = connection;

                } else if (lastConnection != connection || !adapter.isJoinSupported()) {
                    results.add(list);
                    list = new ArrayList<SourceRef>();
                    lastConnection = connection;
                }

                list.add(sourceRef);
            }
        }

        if (!list.isEmpty()) results.add(list);

        return results;
    }

    public List<Collection<SourceRef>> getLocalGroupsOfSources(
            Partition partition,
            EntryMapping baseMapping,
            EntryMapping entryMapping
    ) throws Exception {

        if (entryMapping == baseMapping) {
            return getGroupsOfSources(partition, entryMapping);
        }

        List<Collection<SourceRef>> results = new ArrayList<Collection<SourceRef>>();

        Collection<SourceRef> list = new ArrayList<SourceRef>();
        Connection lastConnection = null;

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        for (EntryMapping em : partitionConfig.getDirectoryConfigs().getRelativePath(baseMapping, entryMapping)) {
            if (em == baseMapping) continue;
            
            for (SourceRef sourceRef : partition.getSourceRefs(em)) {

                Source source = sourceRef.getSource();
                Connection connection = source.getConnection();
                Adapter adapter = connection.getAdapter();

                if (lastConnection == null) {
                    lastConnection = connection;

                } else if (lastConnection != connection || !adapter.isJoinSupported()) {
                    results.add(list);
                    list = new ArrayList<SourceRef>();
                    lastConnection = connection;
                }

                list.add(sourceRef);
            }
        }

        if (!list.isEmpty()) results.add(list);

        return results;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }
}

