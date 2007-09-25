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
import org.safehaus.penrose.partition.Partitions;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.SourceValues;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

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
            Entry entry,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entry.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            Entry parent = entry.getParent();
            DN parentDn = parent.getDn();

            SearchResult sr = find(session, partition, parent, parentDn);
            sourceValues.add(sr.getSourceValues());

        } else {
            EngineTool.extractSourceValues(entry, dn, sourceValues);
        }

        EngineTool.propagateDown(entry, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Partitions partitions = penroseContext.getPartitions();
        Engine engine = partitions.getEngine(partition, this, entry);
        engine.add(session, entry, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            Partition partition,
            Entry entry,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entry.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult sr = find(null, partition, entry, dn);
            sourceValues.add(sr.getSourceValues());
        } else {
            EngineTool.extractSourceValues(entry, dn, sourceValues);
        }

        EngineTool.propagateDown(entry, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Partitions partitions = penroseContext.getPartitions();
        Engine engine = partitions.getEngine(partition, this, entry);
        engine.bind(session, entry, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            Partition partition,
            Entry entry,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entry.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult sr = find(null, partition, entry, dn);
            sourceValues.add(sr.getSourceValues());
        } else {
            EngineTool.extractSourceValues(entry, dn, sourceValues);
        }

        EngineTool.propagateDown(entry, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Partitions partitions = penroseContext.getPartitions();
        Engine engine = partitions.getEngine(partition, this, entry);
        engine.compare(session, entry, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unbind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind(
            Session session,
            Partition partition,
            Entry entry,
            UnbindRequest request,
            UnbindResponse response
    ) throws Exception {

        //Partitions partitions = penroseContext.getPartitions();
        //Engine engine = getEngine(partition, this, entry.getEntryMapping());

        //engine.unbind(session, partition, entry, bindDn);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResult find(
            Session session,
            Partition partition,
            Entry entry,
            DN dn
    ) throws Exception {

        Partitions partitions = penroseContext.getPartitions();
        Engine engine = partitions.getEngine(partition, this, entry);
        return engine.find(session, entry, dn);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Partition partition,
            Entry entry,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entry.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult sr = find(session, partition, entry, dn);
            sourceValues.add(sr.getSourceValues());
        } else {
            EngineTool.extractSourceValues(entry, dn, sourceValues);
        }

        EngineTool.propagateDown(entry, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Partitions partitions = penroseContext.getPartitions();
        Engine engine = partitions.getEngine(partition, this, entry);
        engine.delete(session, entry, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            Partition partition,
            Entry entry,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entry.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult sr = find(session, partition, entry, dn);
            sourceValues.add(sr.getSourceValues());
        } else {
            EngineTool.extractSourceValues(entry, dn, sourceValues);
        }

        EngineTool.propagateDown(entry, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Partitions partitions = penroseContext.getPartitions();
        Engine engine = partitions.getEngine(partition, this, entry);
        engine.modify(session, entry, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            Partition partition,
            Entry entry,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entry.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult sr = find(session, partition, entry, dn);
            sourceValues.add(sr.getSourceValues());
        } else {
            EngineTool.extractSourceValues(entry, dn, sourceValues);
        }

        EngineTool.propagateDown(entry, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Partitions partitions = penroseContext.getPartitions();
        Engine engine = partitions.getEngine(partition, this, entry);
        engine.modrdn(session, entry, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract void search(
            Session session,
            Entry base,
            Entry entry,
            SourceValues sourceValues,
            SearchRequest request,
            SearchResponse response
    ) throws Exception;

    public void performSearch(
            final Session session,
            final Entry base,
            final Entry entry,
            SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        Partitions partitions = penroseContext.getPartitions();
        Engine engine = partitions.getEngine(partition, this, entry);
        engine.search(
                session,
                base,
                entry,
                sourceValues,
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

