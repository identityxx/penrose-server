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

import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.matchingRule.EqualityMatchingRule;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connector.ConnectorManager;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceManager;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.adapter.Adapter;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public abstract class Engine {

    public Logger log = LoggerFactory.getLogger(getClass());

    public PenroseConfig penroseConfig;
    public PenroseContext penroseContext;
    public SessionContext sessionContext;

    public EngineConfig engineConfig;

    public SchemaManager schemaManager;
    public InterpreterManager interpreterManager;
    public ConnectorManager connectorManager;
    public ConnectionManager connectionManager;
    public PartitionManager partitionManager;

    public boolean stopping = false;

    protected Analyzer analyzer;

    public Collection<String> getParameterNames() {
        return engineConfig.getParameterNames();
    }

    public String getParameter(String name) {
        return engineConfig.getParameter(name);
    }

    public void init() throws Exception {

        analyzer = new Analyzer();
        analyzer.setPartitionManager(partitionManager);
        analyzer.setInterpreterManager(interpreterManager);
    }

    public EngineConfig getEngineConfig() {
        return engineConfig;
    }

    public void setEngineConfig(EngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
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

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) throws Exception {
        this.partitionManager = partitionManager;
    }

    public SourceMapping getPrimarySource(EntryMapping entryMapping) throws Exception {
        return analyzer.getPrimarySource(entryMapping);
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

    public Graph getGraph(EntryMapping entryMapping) throws Exception {
        return analyzer.getGraph(entryMapping);
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {

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

    public Filter generateFilter(SourceMapping sourceMapping, Collection relationships, Collection rows) throws Exception {
        log.debug("Generating filters for source "+sourceMapping.getName());

        Filter filter = null;
        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            RDN rdn = (RDN)i.next();
            log.debug(" - "+rdn);

            Filter subFilter = null;

            for (Iterator j=relationships.iterator(); j.hasNext(); ) {
                Relationship relationship = (Relationship)j.next();

                String lhs = relationship.getLhs();
                String operator = relationship.getOperator();
                String rhs = relationship.getRhs();

                if (rhs.startsWith(sourceMapping.getName()+".")) {
                    String exp = lhs;
                    lhs = rhs;
                    rhs = exp;
                }

                int index = lhs.indexOf(".");
                String name = lhs.substring(index+1);

                log.debug("   - "+rhs+" ==> ("+name+" "+operator+" ?)");
                Object value = rdn.get(rhs);
                if (value == null) continue;

                SimpleFilter sf = new SimpleFilter(name, operator, value.toString());
                log.debug("     --> "+sf);

                subFilter = FilterTool.appendAndFilter(subFilter, sf);
            }

            filter = FilterTool.appendOrFilter(filter, subFilter);
        }

        return filter;
    }

    public Filter generateFilter(
            SourceMapping toSource,
            Collection relationships,
            SourceValues sv
    ) throws Exception {
/*
        log.debug("Generating filters using source values:");
        for (Iterator i=sv.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = sv.get(name);
            log.debug(" - "+name+": "+values);
        }
*/
        Filter filter = null;

        for (Iterator j=relationships.iterator(); j.hasNext(); ) {
            Relationship relationship = (Relationship)j.next();
            //log.debug("Relationship "+relationship);

            String lhs = relationship.getLhs();
            String operator = relationship.getOperator();
            String rhs = relationship.getRhs();

            if (rhs.startsWith(toSource.getName()+".")) {
                String exp = lhs;
                lhs = rhs;
                rhs = exp;
            }

            int lindex = lhs.indexOf(".");
            //String lsourceName = lhs.substring(0, lindex);
            String lname = lhs.substring(lindex+1);

            //int rindex = rhs.indexOf(".");
            //String rsourceName = rhs.substring(0, rindex);
            //String rname = rhs.substring(rindex+1);

            //log.debug("   converting "+rhs+" ==> ("+lname+" "+operator+" ?)");

            Collection v = null; //sv.get(rhs);
            //log.debug("   - found "+v);
            if (v == null) continue;

            Filter orFilter = null;
            for (Iterator k=v.iterator(); k.hasNext(); ) {
                Object value = k.next();

                SimpleFilter sf = new SimpleFilter(lname, operator, value.toString());
                //log.debug("   - "+sf);

                orFilter = FilterTool.appendOrFilter(orFilter, sf);
            }
            log.debug("   - "+orFilter);

            filter = FilterTool.appendAndFilter(filter, orFilter);
        }

        return filter;
    }

    public boolean isStopping() {
        return stopping;
    }

    public boolean isStatic(Partition partition, EntryMapping entryMapping) throws Exception {
        Collection effectiveSources = partition.getEffectiveSourceMappings(entryMapping);
        if (effectiveSources.size() > 0) return false;

        Collection attributeDefinitions = entryMapping.getAttributeMappings();
        for (Iterator i=attributeDefinitions.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            if (attributeMapping.getConstant() == null) return false;
        }

        EntryMapping parentMapping = partition.getParent(entryMapping);
        if (parentMapping == null) return true;

        return isStatic(partition, parentMapping);
    }

    public boolean isUnique(Partition partition, EntryMapping entryMapping) throws Exception {
        return analyzer.isUnique(partition, entryMapping);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract void add(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
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
            BindRequest request,
            BindResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = request.getDn();
        byte[] password = request.getPassword();

        SearchResult searchResult = find(session, partition, entryMapping, dn);

        Attributes attributes = searchResult.getAttributes();
        Attribute attribute = attributes.get("userPassword");

        if (attribute == null) {
            log.debug("Attribute userPassword not found");
            throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
        }

        Collection<Object> userPasswords = attribute.getValues();
        for (Object userPassword : userPasswords) {
            if (debug) log.debug("userPassword: " + userPassword);
            if (PasswordUtil.comparePassword(password, userPassword)) return;
        }

        throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean compare(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = request.getDn();
        String attributeName = request.getAttributeName();
        Object attributeValue = request.getAttributeValue();

        SearchResult searchResult = find(session, partition, entryMapping, dn);

        Attributes attributes = searchResult.getAttributes();
        Attribute attribute = attributes.get(attributeName);
        if (attribute == null) {
            if (debug) log.debug("Attribute "+attributeName+" not found.");
            return false;
        }

        Collection<Object> values = attribute.getValues();
        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        String equality = attributeType == null ? null : attributeType.getEquality();
        EqualityMatchingRule equalityMatchingRule = EqualityMatchingRule.getInstance(equality);

        if (debug) log.debug("Comparing values:");
        for (Object value : values) {
            boolean b = equalityMatchingRule.compare(value, attributeValue);
            if (debug) log.debug(" - [" + value + "] => " + b);

            if (b) return true;

        }

        return false;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract void delete(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
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

        boolean debug = log.isDebugEnabled();

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setScope(SearchRequest.SCOPE_BASE);

        SearchResponse<SearchResult> response = new SearchResponse<SearchResult>();

        search(
                session,
                partition,
                entryMapping,
                request,
                response
        );

        if (!response.hasNext()) {
            if (debug) log.debug("Entry "+dn+" not found");
            throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
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
            SearchResponse<SearchResult> response
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
            SearchResponse<SearchResult> response
    ) throws Exception;

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

        Collection<FieldMapping> fields = partition.getSearchableFields(sourceMapping);

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

        EntryMapping parentMapping = partition.getParent(entryMapping);

        Collection<DN> parentDns = new ArrayList<DN>();
        if (parentMapping != null) {
            computeDns(partition, interpreter, parentMapping, parentDns);

        } else if (!entryMapping.getParentDn().isEmpty()) {
            parentDns.add(entryMapping.getParentDn());
        }

        if (parentDns.isEmpty()) {
            DN dn = entryMapping.getDn();
            log.debug("DN: "+dn);
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

                    log.debug("DN: " + dn);
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
        Attributes rdns = new Attributes();

        Collection<AttributeMapping> rdnAttributes = entryMapping.getRdnAttributeMappings();
        for (AttributeMapping attributeMapping : rdnAttributes) {
            String name = attributeMapping.getName();

            Object value = interpreter.eval(attributeMapping);
            if (value == null) continue;

            rdns.addValue(name, value);
        }

        return TransformEngine.convert(rdns);
    }

    public PenroseConfig getServerConfig() {
        return penroseConfig;
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
        connectionManager =penroseContext.getConnectionManager();
        partitionManager = penroseContext.getPartitionManager();
    }

    public List<Collection<SourceRef>> getGroupsOfSources(
            Partition partition,
            EntryMapping entryMapping
    ) throws Exception {

        List<Collection<SourceRef>> results = new ArrayList<Collection<SourceRef>>();

        SourceManager sourceManager = penroseContext.getSourceManager();
        Collection<SourceRef> list = new ArrayList<SourceRef>();
        Connection lastConnection = null;

        for (EntryMapping em : partition.getPath(entryMapping)) {

            for (SourceRef sourceRef : sourceManager.getSourceRefs(partition, em)) {

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

        SourceManager sourceManager = penroseContext.getSourceManager();
        Collection<SourceRef> list = new ArrayList<SourceRef>();
        Connection lastConnection = null;

        for (EntryMapping em : partition.getRelativePath(baseMapping, entryMapping)) {
            if (em == baseMapping) continue;
            
            for (SourceRef sourceRef : sourceManager.getSourceRefs(partition, em)) {

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

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public void setSessionContext(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
    }
}

