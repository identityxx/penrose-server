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
package org.safehaus.penrose.handler;

import org.safehaus.penrose.session.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.Partitions;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.SourceValues;
import org.safehaus.penrose.source.SourceConfigs;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.FieldConfig;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public abstract class Handler {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    public final static String  FETCH         = "fetch";
    public final static boolean DEFAULT_FETCH = false;

    protected HandlerConfig      handlerConfig;
    protected Partition          partition;
    protected PenroseContext     penroseContext;

    protected boolean fetch = DEFAULT_FETCH;

    public Handler() throws Exception {
    }

    public void init(HandlerConfig handlerConfig) throws Exception {

        log.debug("Initializing handler "+handlerConfig.getName()+".");

        this.handlerConfig = handlerConfig;

        init();
    }

    public void init() throws Exception {
    }

    public String getName() {
        return handlerConfig.getName();
    }
    
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entryMapping.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            PartitionConfig partitionConfig = partition.getPartitionConfig();
            EntryMapping parentMapping = partitionConfig.getDirectoryConfigs().getParent(entryMapping);
            DN parentDn = dn.getParentDn();

            SearchResult parent = find(session, partition, parentMapping, parentDn);
            sourceValues.add(parent.getSourceValues());

        } else {
            extractSourceValues(partition, entryMapping, dn, sourceValues);
        }

        EngineTool.propagateDown(partition, entryMapping, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Partitions partitions = penroseContext.getPartitions();
        Engine engine = partitions.getEngine(partition, this, entryMapping);
        engine.add(session, partition, entryMapping, sourceValues, request, response);
    }

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

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entryMapping.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult entry = find(null, partition, entryMapping, dn);
            sourceValues.add(entry.getSourceValues());
        } else {
            extractSourceValues(partition, entryMapping, dn, sourceValues);
        }

        EngineTool.propagateDown(partition, entryMapping, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Partitions partitions = penroseContext.getPartitions();
        Engine engine = partitions.getEngine(partition, this, entryMapping);
        engine.bind(session, partition, entryMapping, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entryMapping.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult entry = find(null, partition, entryMapping, dn);
            sourceValues.add(entry.getSourceValues());
        } else {
            extractSourceValues(partition, entryMapping, dn, sourceValues);
        }

        EngineTool.propagateDown(partition, entryMapping, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Partitions partitions = penroseContext.getPartitions();
        Engine engine = partitions.getEngine(partition, this, entryMapping);
        engine.compare(session, partition, entryMapping, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unbind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            UnbindRequest request,
            UnbindResponse response
    ) throws Exception {

        //Partitions partitions = penroseContext.getPartitions();
        //Engine engine = getEngine(partition, this, entryMapping);

        //engine.unbind(session, partition, entryMapping, bindDn);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResult find(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            DN dn
    ) throws Exception {

        Partitions partitions = penroseContext.getPartitions();
        Engine engine = partitions.getEngine(partition, this, entryMapping);
        return engine.find(session, partition, entryMapping, dn);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entryMapping.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult entry = find(session, partition, entryMapping, dn);
            sourceValues.add(entry.getSourceValues());
        } else {
            extractSourceValues(partition, entryMapping, dn, sourceValues);
        }

        EngineTool.propagateDown(partition, entryMapping, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Partitions partitions = penroseContext.getPartitions();
        Engine engine = partitions.getEngine(partition, this, entryMapping);
        engine.delete(session, partition, entryMapping, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entryMapping.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult entry = find(session, partition, entryMapping, dn);
            sourceValues.add(entry.getSourceValues());
        } else {
            extractSourceValues(partition, entryMapping, dn, sourceValues);
        }

        EngineTool.propagateDown(partition, entryMapping, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Partitions partitions = penroseContext.getPartitions();
        Engine engine = partitions.getEngine(partition, this, entryMapping);
        engine.modify(session, partition, entryMapping, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entryMapping.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult entry = find(session, partition, entryMapping, dn);
            sourceValues.add(entry.getSourceValues());
        } else {
            extractSourceValues(partition, entryMapping, dn, sourceValues);
        }

        EngineTool.propagateDown(partition, entryMapping, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Partitions partitions = penroseContext.getPartitions();
        Engine engine = partitions.getEngine(partition, this, entryMapping);
        engine.modrdn(session, partition, entryMapping, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SearchRequest request,
            SearchResponse results
    ) throws Exception {

        search(
                session,
                partition,
                entryMapping,
                entryMapping,
                request,
                results
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

    public void performSearch(
            final Session session,
            final Partition partition,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        Partitions partitions = penroseContext.getPartitions();
        Engine engine = partitions.getEngine(partition, this, entryMapping);
        engine.search(
                session,
                partition,
                baseMapping,
                entryMapping,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Miscelleanous
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public HandlerConfig getHandlerConfig() {
        return handlerConfig;
    }

    public void setHandlerConfig(HandlerConfig handlerConfig) {
        this.handlerConfig = handlerConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
    }

    public void extractSourceValues(
            Partition partition,
            EntryMapping entryMapping,
            DN dn,
            SourceValues sourceValues
    ) throws Exception {

        InterpreterManager interpreterManager = penroseContext.getInterpreterManager();
        Interpreter interpreter = interpreterManager.newInstance();

        if (debug) log.debug("Extracting source values from "+dn);

        extractSourceValues(
                partition,
                interpreter,
                dn,
                entryMapping,
                sourceValues
        );
    }

    public void extractSourceValues(
            Partition partition,
            Interpreter interpreter,
            DN dn,
            EntryMapping entryMapping,
            SourceValues sourceValues
    ) throws Exception {

        DN parentDn = dn.getParentDn();
        PartitionConfig partitionConfig = partition.getPartitionConfig();
        EntryMapping em = partitionConfig.getDirectoryConfigs().getParent(entryMapping);

        if (parentDn != null && em != null) {
            extractSourceValues(partition, interpreter, parentDn, em, sourceValues);
        }

        RDN rdn = dn.getRdn();
        Collection<SourceMapping> sourceMappings = entryMapping.getSourceMappings();

        //if (sourceMappings.isEmpty()) return;
        //SourceMapping sourceMapping = sourceMappings.iterator().next();

        //interpreter.set(sourceValues);
        interpreter.set(rdn);

        for (SourceMapping sourceMapping : sourceMappings) {
            extractSourceValues(
                    partition,
                    interpreter,
                    rdn,
                    entryMapping,
                    sourceMapping,
                    sourceValues
            );
        }

        interpreter.clear();
    }

    public void extractSourceValues(
            Partition partition,
            Interpreter interpreter,
            RDN rdn,
            EntryMapping entryMapping,
            SourceMapping sourceMapping,
            SourceValues sourceValues
    ) throws Exception {

        if (debug) log.debug("Extracting source "+sourceMapping.getName()+" from RDN: "+rdn);

        Attributes attributes = sourceValues.get(sourceMapping.getName());

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        SourceConfigs sources = partitionConfig.getSourceConfigs();
        SourceConfig sourceConfig = sources.getSourceConfig(sourceMapping.getSourceName());

        Collection<FieldMapping> fieldMappings = sourceMapping.getFieldMappings();
        for (FieldMapping fieldMapping : fieldMappings) {
            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldMapping.getName());

            Object value = interpreter.eval(fieldMapping);
            if (value == null) continue;

            if ("INTEGER".equals(fieldConfig.getType()) && value instanceof String) {
                value = Integer.parseInt((String)value);
            }

            attributes.addValue(fieldMapping.getName(), value);

            String fieldName = sourceMapping.getName() + "." + fieldMapping.getName();
            if (debug) log.debug(" => " + fieldName + ": " + value);
        }
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public String getEngineName() {
        return null;
    }
}

