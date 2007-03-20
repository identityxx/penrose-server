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
package org.safehaus.penrose.engine.impl;

import org.safehaus.penrose.session.*;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.handler.HandlerManager;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineFilterTool;
import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.engine.EntryData;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.entry.Attribute;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EngineImpl extends Engine {

    AddEngine addEngine;
    DeleteEngine deleteEngine;
    ModifyEngine modifyEngine;
    ModRdnEngine modrdnEngine;
    SearchEngine searchEngine;

    LoadEngine loadEngine;
    MergeEngine mergeEngine;
    JoinEngine joinEngine;

    public void init() throws Exception {
        super.init();

        engineFilterTool = new EngineFilterTool(this);
        addEngine        = new AddEngine(this);
        deleteEngine     = new DeleteEngine(this);
        modifyEngine     = new ModifyEngine(this);
        modrdnEngine     = new ModRdnEngine(this);
        searchEngine     = new SearchEngine(this);
        loadEngine       = new LoadEngine(this);
        mergeEngine      = new MergeEngine(this);
        joinEngine       = new JoinEngine(this);
        transformEngine  = new TransformEngine(this);

        log.debug("Default engine initialized.");
    }

    public SearchEngine getSearchEngine() {
        return searchEngine;
    }

    public void setSearchEngine(SearchEngine searchEngine) {
        this.searchEngine = searchEngine;
    }

    public void start() throws Exception {
        super.start();

        //log.debug("Starting Engine...");

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();

            for (Iterator j=partition.getRootEntryMappings().iterator(); j.hasNext(); ) {
                EntryMapping entryMapping = (EntryMapping)j.next();
                analyzer.analyze(partition, entryMapping);
            }
        }

        //threadManager.execute(new RefreshThread(this));

        //log.debug("Engine started.");
    }

    public void stop() throws Exception {
        if (stopping) return;

        log.debug("Stopping Engine...");
        stopping = true;

        // wait for all the worker threads to finish
        //if (threadManager != null) threadManager.stopRequestAllWorkers();
        log.debug("Engine stopped.");
        super.stop();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            Partition partition,
            Entry parent,
            EntryMapping entryMapping,
            AddRequest request,
            AddResponse response
    ) throws LDAPException {

        DN dn = request.getDn();
        Attributes attributes = request.getAttributes();

        AttributeValues attributeValues = EntryUtil.computeAttributeValues(attributes);
        try {
            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("ADD", 80));
                log.debug(Formatter.displayLine("DN: "+entryMapping.getDn(), 80));

                log.debug(Formatter.displayLine("Attribute values:", 80));
                for (Iterator i = attributeValues.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Collection values = attributeValues.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                }

                log.debug(Formatter.displaySeparator(80));
            }

            addEngine.add(partition, parent, entryMapping, dn, attributeValues);

        } catch (LDAPException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
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
    ) throws LDAPException {

        try {
            DN dn = request.getDn();
            String password = request.getPassword();

            log.debug("Bind as user "+dn);

            RDN rdn = dn.getRdn();

            AttributeValues attributeValues = new AttributeValues();
            attributeValues.add(rdn);

            Collection sources = entryMapping.getSourceMappings();
            if (sources.isEmpty()) {
                staticBind(session, partition, entryMapping, dn, password);
                return;
            }

            for (Iterator i=sources.iterator(); i.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)i.next();

                SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

                Map entries = transformEngine.split(
                        partition,
                        entryMapping,
                        sourceMapping,
                        dn,
                        attributeValues
                );

                for (Iterator j=entries.keySet().iterator(); j.hasNext(); ) {
                    RDN pk = (RDN)j.next();
                    //AttributeValues sourceValues = (AttributeValues)entries.get(pk);

                    log.debug("Bind to "+sourceMapping.getName()+" as "+pk+".");

                    Connector connector = getConnector(sourceConfig);
                    connector.bind(partition, sourceConfig, entryMapping, pk, request, response);
                }
            }

            throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);

        } catch (LDAPException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    public void staticBind(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            DN dn,
            String password
    ) throws LDAPException {

        try {
            SearchRequest request = new SearchRequest();
            request.setDn(dn);
            request.setFilter((Filter)null);
            request.setScope(SearchRequest.SCOPE_BASE);

            SearchResponse response = new SearchResponse();

            search(
                    session,
                    partition,
                    new AttributeValues(),
                    entryMapping,
                    request,
                    response
            );

            if (!response.hasNext()) {
                throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
            }

            Entry entry = (Entry) response.next();

            Attributes attributes = entry.getAttributes();
            Attribute attribute = attributes.get("userPassword");

            if (attribute == null) {
                log.debug("Attribute userPassword not found");
                throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
            }

            Collection userPasswords = attribute.getValues();
            for (Iterator j = userPasswords.iterator(); j.hasNext(); ) {
                Object userPassword = j.next();
                log.debug("userPassword: "+userPassword);
                if (PasswordUtil.comparePassword(password, userPassword)) return;
            }

        } catch (LDAPException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            DeleteRequest request,
            DeleteResponse response
    ) throws LDAPException {

        try {
            DN dn = request.getDn();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("DELETE", 80));
                log.debug(Formatter.displayLine("DN: "+entry.getDn(), 80));

                log.debug(Formatter.displaySeparator(80));
            }

            deleteEngine.delete(partition, entry);

        } catch (LDAPException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            ModifyRequest request,
            ModifyResponse response
    ) throws LDAPException {

        try {
            DN dn = request.getDn();
            Collection modifications = request.getModifications();

            AttributeValues oldValues = new AttributeValues();
            for (Iterator i=entry.getAttributes().getAll().iterator(); i.hasNext(); ) {
                Attribute attribute = (Attribute)i.next();
                oldValues.set(attribute.getName(), attribute.getValues());
            }

            log.debug("Old entry:");
            entry.getAttributes().print();

            log.debug("--- perform modification:");
            AttributeValues newValues = new AttributeValues(oldValues);

            Collection objectClasses = getSchemaManager().getObjectClasses(entryMapping);
            Collection objectClassNames = new ArrayList();
            for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
                ObjectClass oc = (ObjectClass)i.next();
                objectClassNames.add(oc.getName());
            }
            log.debug("Object classes: "+objectClassNames);

            for (Iterator i = modifications.iterator(); i.hasNext();) {
                Modification modification = (Modification)i.next();

                int type = modification.getType();
                Attribute attribute = modification.getAttribute();
                String attributeName = attribute.getName();

                if (attributeName.equals("objectClass")) {
                    throw ExceptionUtil.createLDAPException(LDAPException.OBJECT_CLASS_MODS_PROHIBITED);
                }

                Collection values = attribute.getValues();
                Set newAttrValues = new HashSet();
                newAttrValues.addAll(values);

                Collection value = newValues.get(attributeName);
                log.debug("old value " + attributeName + ": "
                        + newValues.get(attributeName));

                Set newValue = new HashSet();
                if (value != null) newValue.addAll(value);

                switch (type) {
                    case Modification.ADD:
                        newValue.addAll(newAttrValues);
                        break;
                    case Modification.DELETE:
                        if (values.isEmpty()) {
                            newValue.clear();
                        } else {
                            newValue.removeAll(newAttrValues);
                        }
                        break;
                    case Modification.REPLACE:
                        newValue = newAttrValues;
                        break;
                }

                newValues.set(attributeName, newValue);

                log.debug("new value " + attributeName + ": "
                        + newValues.get(attributeName));
            }

            Attributes attributes = EntryUtil.computeAttributes(newValues);
            Entry newEntry = new Entry(entry.getDn(), entryMapping, attributes, entry.getSourceValues());

            log.debug("New entry:");
            newEntry.getAttributes().print();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("MODIFY", 80));
                log.debug(Formatter.displayLine("DN: "+entry.getDn(), 80));

                log.debug(Formatter.displayLine("Old attribute values:", 80));
                for (Iterator iterator = oldValues.getNames().iterator(); iterator.hasNext(); ) {
                    String name = (String)iterator.next();
                    Collection values = oldValues.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                }

                log.debug(Formatter.displayLine("New attribute values:", 80));
                for (Iterator iterator = newValues.getNames().iterator(); iterator.hasNext(); ) {
                    String name = (String)iterator.next();
                    Collection values = newValues.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                }

                log.debug(Formatter.displaySeparator(80));
            }

            modifyEngine.modify(partition, entry, newValues);

        } catch (LDAPException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws LDAPException {

        try {
            DN dn = request.getDn();
            RDN newRdn = request.getNewRdn();
            boolean deleteOldRdn = request.getDeleteOldRdn();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("MODRDN", 80));
                log.debug(Formatter.displayLine("DN: "+entry.getDn(), 80));
                log.debug(Formatter.displayLine("New RDN: "+newRdn, 80));
                log.debug(Formatter.displayLine("Delete old RDN: "+deleteOldRdn, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            modrdnEngine.modrdn(partition, entry, newRdn, deleteOldRdn);

        } catch (LDAPException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public List find(
            Partition partition,
            AttributeValues sourceValues,
            EntryMapping entryMapping,
            List rdns,
            int position
    ) throws Exception {

        DNBuilder db = new DNBuilder();
        for (int i = rdns.size()-position-1; i < rdns.size(); i++) {
            db.append((RDN)rdns.get(i));
        }
        DN dn = db.toDn();

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("FIND", 80));
            log.debug(Formatter.displayLine("DN: "+dn, 80));
            log.debug(Formatter.displayLine("Mapping: "+entryMapping.getDn(), 80));

            if (!sourceValues.isEmpty()) {
                log.debug(Formatter.displayLine("Source values:", 80));
                for (Iterator i = sourceValues.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Collection values = sourceValues.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                }
            }

            log.debug(Formatter.displaySeparator(80));
        }

        List path = new ArrayList();

        RDN rdn = dn.getRdn();

        EntryData data = new EntryData();
        data.setDn(dn);
        data.setFilter(rdn);

        Collection list = new ArrayList();
        list.add(data);

        AttributeValues loadedSourceValues = loadEngine.loadEntries(
                partition,
                sourceValues,
                entryMapping,
                list
        );

        if (loadedSourceValues != null) {

            final Interpreter interpreter = getInterpreterManager().newInstance();

            SourceMapping primarySourceMapping = getPrimarySource(entryMapping);

            RDN filter = createFilter(partition, interpreter, primarySourceMapping, entryMapping, rdn);

            for (Iterator i=loadedSourceValues.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                Collection c = loadedSourceValues.get(name);
                for (Iterator j=c.iterator(); j.hasNext(); ) {
                    AttributeValues sv = (AttributeValues)j.next();
                    sourceValues.add(sv);
                }
            }

            Entry entry = mergeEngine.mergeEntries(
                    partition,
                    dn,
                    entryMapping,
                    sourceValues,
                    new AttributeValues(),
                    new ArrayList(),
                    interpreter,
                    filter
            );

            path.add(entry);
            if (entry != null) sourceValues.add(entry.getSourceValues());
        }

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("FIND RESULT", 80));

            if (path.isEmpty()) {
                log.debug(Formatter.displayLine("Entry \""+dn+"\" not found", 80));
            } else {
                for (Iterator i=path.iterator(); i.hasNext(); ) {
                    Entry entry = (Entry)i.next();
                    log.debug(Formatter.displayLine(" - "+(entry == null ? null : entry.getDn()), 80));
                }
            }

            if (!sourceValues.isEmpty()) {
                log.debug(Formatter.displayLine("Source values:", 80));
                for (Iterator i = sourceValues.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Collection values = sourceValues.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                }
            }

            log.debug(Formatter.displaySeparator(80));
        }

        return path;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            final Session session,
            final Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        final boolean debug = log.isDebugEnabled();
        final DN baseDn = request.getDn();
        final Filter filter = request.getFilter();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH", 80));
            log.debug(Formatter.displayLine("Mapping DN: \""+entryMapping.getDn()+"\"", 80));
            log.debug(Formatter.displayLine("Base DN: "+baseDn, 80));
            log.debug(Formatter.displayLine("Filter: "+filter, 80));
            log.debug(Formatter.displayLine("Scope: "+LDAPUtil.getScope(request.getScope()), 80));
            log.debug(Formatter.displayLine("Parent source values:", 80));

            if (sourceValues != null) {
                for (Iterator i = sourceValues.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Collection values = sourceValues.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                }
            }

            log.debug(Formatter.displaySeparator(80));
        }

        int rc = LDAPException.SUCCESS;

        try {

            if (request.getScope() == SearchRequest.SCOPE_BASE) {
                if (!(entryMapping.getDn().matches(baseDn))) {
                    return;
                }

            } else if (request.getScope() == SearchRequest.SCOPE_ONE) {
                if (!entryMapping.getParentDn().matches(baseDn)) {
                    return;
                }

            } else { // if (request.getScope() == SearchRequest.SCOPE_SUB) {

                log.debug("Checking whether "+baseDn+" is an ancestor of "+entryMapping.getDn());
                DN dn = entryMapping.getDn();
                boolean found = false;
                while (dn != null) {
                    if (baseDn.matches(dn)) {
                        found = true;
                        break;
                    }
                    dn = dn.getParentDn();
                }

                log.debug("Result: "+found);

                if (!found) {
                    return;
                }
            }

            final boolean unique = isUnique(partition, entryMapping);
            final Collection effectiveSources = partition.getEffectiveSourceMappings(entryMapping);

            final SearchResponse entriesToLoad = new SearchResponse();
            final SearchResponse loadedEntries = new SearchResponse();

            HandlerManager handlerManager = penroseContext.getHandlerManager();
            final Handler handler = handlerManager.getHandler("DEFAULT");

            final Interpreter interpreter = getInterpreterManager().newInstance();

            Collection attributeNames = request.getAttributes();
            Collection attributeDefinitions = entryMapping.getAttributeMappings(attributeNames);

            // check if client only requests the dn to be returned
            final boolean dnOnly = attributeNames != null && attributeNames.contains("dn")
                    && attributeDefinitions.isEmpty()
                    && "(objectclass=*)".equals(filter.toString().toLowerCase());

            log.debug("Search DNs only: "+dnOnly);

            final SearchResponse dns = new SearchResponse() {
                public void add(Object object) throws Exception {
                    EntryData data = (EntryData)object;
                    DN dn = data.getDn();

                    if (dnOnly) {
                        log.debug("Returning DN only.");

                        AttributeValues sv = data.getMergedValues();
                        Entry entry = new Entry(dn, entryMapping, null, sv);

                        response.add(entry);
                        return;
                    }

                    if (unique && effectiveSources.size() == 1 && !data.getMergedValues().isEmpty()) {
                        log.debug("Entry data is complete, returning entry.");

                        AttributeValues sv = data.getMergedValues();
                        Attributes attributes = createAttributes(entryMapping, sv, interpreter);
                        Entry entry = new Entry(dn, entryMapping, attributes, sv);

                        log.debug("Checking filter "+filter+" on "+entry.getDn());
                        if (handler.getFilterTool().isValid(entry, filter)) {
                            response.add(entry);
                        } else {
                            log.debug("Entry \""+entry.getDn()+"\" doesn't match search filter.");
                        }

                        return;
                    }

                    log.debug("Entry data is incomplete, loading full entry data.");
                    entriesToLoad.add(data);
                }

                public void close() throws Exception {
                    if (!dnOnly) {
                        entriesToLoad.close();
                    }
                }
            };

            log.debug("Filter cache for "+filter+" not found.");

            searchEngine.search(partition, sourceValues, entryMapping, filter, dns);

            dns.close();

            if (dnOnly) return;
            //if (unique && effectiveSources.size() == 1) return LDAPException.SUCCESS;

            load(partition, entryMapping, entriesToLoad, loadedEntries);

            final SearchResponse newEntries = new SearchResponse() {
                public void add(Object object) throws Exception {
                    Entry entry = (Entry)object;

                    log.debug("Checking filter "+filter+" on "+entry.getDn());
                    if (handler.getFilterTool().isValid(entry, filter)) {
                        response.add(entry);
                    } else {
                        log.debug("Entry \""+entry.getDn()+"\" doesn't match search filter.");
                    }
                }
            };

            merge(partition, entryMapping, loadedEntries, newEntries);

        } finally {
            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("EXPAND MAPPING", 80));
                log.debug(Formatter.displayLine("Return code: "+rc, 80));
                log.debug(Formatter.displaySeparator(80));
            }
        }
    }

    public void load(
            Partition partition,
            EntryMapping entryMapping,
            SearchResponse entriesToLoad,
            SearchResponse loadedEntries)
            throws Exception {

        loadEngine.load(partition, entryMapping, entriesToLoad, loadedEntries);
    }

    public void merge(
            Partition partition,
            EntryMapping entryMapping,
            SearchResponse loadedEntries,
            SearchResponse newEntries)
            throws Exception {

        mergeEngine.merge(partition, entryMapping, loadedEntries, newEntries);
    }
}