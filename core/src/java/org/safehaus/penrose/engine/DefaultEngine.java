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

import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.util.LDAPUtil;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.handler.FindHandler;
import org.ietf.ldap.LDAPException;

import javax.naming.directory.*;
import javax.naming.NamingEnumeration;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class DefaultEngine extends Engine {

    AddEngine addEngine;
    DeleteEngine deleteEngine;
    ModifyEngine modifyEngine;
    ModRdnEngine modrdnEngine;
    SearchEngine searchEngine;

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

    public int bind(PenroseSession session, Partition partition, EntryMapping entryMapping, String dn, String password) throws Exception {

        log.debug("Bind as user "+dn);

        Row rdn = EntryUtil.getRdn(dn);

        AttributeValues attributeValues = new AttributeValues();
        attributeValues.add(rdn);

        Collection sources = entryMapping.getSourceMappings();

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            SourceMapping source = (SourceMapping)i.next();

            SourceConfig sourceConfig = partition.getSourceConfig(source.getSourceName());

            Map entries = transformEngine.split(partition, entryMapping, source, attributeValues);

            for (Iterator j=entries.keySet().iterator(); j.hasNext(); ) {
                Row pk = (Row)j.next();
                //AttributeValues sourceValues = (AttributeValues)entries.get(pk);

                log.debug("Bind to "+source.getName()+" as "+pk+".");

                int rc = getConnector(sourceConfig).bind(partition, sourceConfig, entryMapping, pk, password);
                if (rc == LDAPException.SUCCESS) return rc;
            }
        }

        return LDAPException.INVALID_CREDENTIALS;
    }

    public int add(
            PenroseSession session, Partition partition,
            Entry parent,
            EntryMapping entryMapping,
            String dn,
            Attributes attributes)
            throws Exception {

        // normalize attribute names
        AttributeValues attributeValues = new AttributeValues();

        for (NamingEnumeration i=attributes.getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            String name = attribute.getID();

            if ("objectClass".equalsIgnoreCase(name)) continue;

            AttributeMapping attributeMapping = entryMapping.getAttributeMapping(name);
            if (attributeMapping == null) {
                log.debug("Undefined attribute "+name);
                return LDAPException.OBJECT_CLASS_VIOLATION;
            }

            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();
                attributeValues.add(name, value);
            }
        }

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

        int rc = addEngine.add(partition, parent, entryMapping, dn, attributeValues);

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ADD RC:"+rc, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        return rc;
    }

    public int delete(PenroseSession session, Partition partition, Entry entry) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("DELETE", 80));
            log.debug(Formatter.displayLine("DN: "+entry.getDn(), 80));

            log.debug(Formatter.displaySeparator(80));
        }

        int rc = deleteEngine.delete(partition, entry);

        return rc;
    }

    public int modrdn(PenroseSession session, Partition partition, Entry entry, String newRdn, boolean deleteOldRdn) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODRDN", 80));
            log.debug(Formatter.displayLine("DN: "+entry.getDn(), 80));
            log.debug(Formatter.displayLine("New RDN: "+newRdn, 80));
            log.debug(Formatter.displayLine("Delete old RDN: "+deleteOldRdn, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        int rc = modrdnEngine.modrdn(partition, entry, newRdn, deleteOldRdn);

        return rc;
    }

    public int modify(PenroseSession session, Partition partition, Entry entry, Collection modifications) throws Exception {

        EntryMapping entryMapping = entry.getEntryMapping();
        AttributeValues oldValues = entry.getAttributeValues();

        log.debug("Old entry:");
        log.debug("\n"+EntryUtil.toString(entry));

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
            ModificationItem modification = (ModificationItem)i.next();

            Attribute attribute = modification.getAttribute();
            String attributeName = attribute.getID();

            if (attributeName.equals("objectClass"))
                return LDAPException.OBJECT_CLASS_MODS_PROHIBITED;

            Set newAttrValues = new HashSet();
            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();
                newAttrValues.add(value);
            }

            Collection value = newValues.get(attributeName);
            log.debug("old value " + attributeName + ": "
                    + newValues.get(attributeName));

            Set newValue = new HashSet();
            if (value != null) newValue.addAll(value);

            switch (modification.getModificationOp()) {
                case DirContext.ADD_ATTRIBUTE:
                    newValue.addAll(newAttrValues);
                    break;
                case DirContext.REMOVE_ATTRIBUTE:
                    if (attribute.get() == null) {
                        newValue.clear();
                    } else {
                        newValue.removeAll(newAttrValues);
                    }
                    break;
                case DirContext.REPLACE_ATTRIBUTE:
                    newValue = newAttrValues;
                    break;
            }

            newValues.set(attributeName, newValue);

            log.debug("new value " + attributeName + ": "
                    + newValues.get(attributeName));
        }

        Entry newEntry = new Entry(entry.getDn(), entryMapping, newValues, entry.getSourceValues());

        log.debug("New entry:");
        log.debug("\n"+EntryUtil.toString(newEntry));

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

        int rc = modifyEngine.modify(partition, entry, newValues);

        return rc;
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

    public List find(
            Partition partition,
            AttributeValues sourceValues,
            EntryMapping entryMapping,
            List rdns,
            int position
    ) throws Exception {

        String dn = null;
        for (int i = rdns.size()-position-1; i < rdns.size(); i++) {
            dn = EntryUtil.append(dn, (Row)rdns.get(i));
        }

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

        Row rdn = EntryUtil.getRdn(dn);

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

            Row filter = createFilter(partition, interpreter, primarySourceMapping, entryMapping, rdn);

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

    public int search(
            PenroseSession session,
            Partition partition,
            AttributeValues sourceValues,
            EntryMapping entryMapping,
            String baseDn,
            Filter filter,
            PenroseSearchControls sc,
            PenroseSearchResults results
    ) throws Exception {

        Handler handler = penrose.getHandler();
        FindHandler findHandler = handler.getFindHandler();

        List path = new ArrayList();
        findHandler.find(partition, baseDn, path, sourceValues);

        Entry entry = (Entry)path.iterator().next();

        if (entry == null) {
            log.debug("Entry "+baseDn+" not found");
            return LDAPException.NO_SUCH_OBJECT;
        }

        if (sc.getScope() == PenroseSearchControls.SCOPE_BASE || sc.getScope() == PenroseSearchControls.SCOPE_SUB) {
            log.debug("Checking filter "+filter+" on "+entry.getDn());
            if (handler.getFilterTool().isValid(entry, filter)) {
                results.add(entry);
            } else {
                log.debug("Base entry \""+entry.getDn()+"\" doesn't match search filter.");
            }
        }

        return LDAPException.SUCCESS;
    }

    public int expand(
            PenroseSession session,
            final Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping entryMapping,
            final String baseDn,
            final Filter filter,
            final PenroseSearchControls sc,
            final PenroseSearchResults results
    ) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("EXPAND MAPPING", 80));
            log.debug(Formatter.displayLine("Mapping DN: \""+entryMapping.getDn()+"\"", 80));
            log.debug(Formatter.displayLine("Base DN: "+baseDn, 80));
            log.debug(Formatter.displayLine("Filter: "+filter, 80));
            log.debug(Formatter.displayLine("Scope: "+LDAPUtil.getScope(sc.getScope()), 80));
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

            if (!getFilterTool().isValid(entryMapping, filter)) {
                return rc;

            } if (sc.getScope() == PenroseSearchControls.SCOPE_BASE) {
                if (!(EntryUtil.match(entryMapping.getDn(), baseDn))) {
                    return rc;
                }

            } else if (sc.getScope() == PenroseSearchControls.SCOPE_ONE) {
                if (!EntryUtil.match(entryMapping.getParentDn(), baseDn)) {
                    return rc;
                }

            } else { // if (sc.getScope() == PenroseSearchControls.SCOPE_SUB) {

                log.debug("Checking whether "+baseDn+" is an ancestor of "+entryMapping.getDn());
                String dn = entryMapping.getDn();
                boolean found = false;
                while (dn != null) {
                    if (EntryUtil.match(dn, baseDn)) {
                        found = true;
                        break;
                    }
                    dn = EntryUtil.getParentDn(dn);
                }

                log.debug("Result: "+found);

                if (!found) {
                    return rc;
                }
            }

            final boolean unique = isUnique(partition, entryMapping);
            final Collection effectiveSources = partition.getEffectiveSourceMappings(entryMapping);

            final PenroseSearchResults dns = new PenroseSearchResults();
            final PenroseSearchResults entriesToLoad = new PenroseSearchResults();
            final PenroseSearchResults loadedEntries = new PenroseSearchResults();
            final PenroseSearchResults newEntries = new PenroseSearchResults();
            final Handler handler = penrose.getHandler();

            final Interpreter interpreter = getInterpreterManager().newInstance();

            Collection attributeNames = sc.getAttributes();
            Collection attributeDefinitions = entryMapping.getAttributeMappings(attributeNames);

            // check if client only requests the dn to be returned
            final boolean dnOnly = attributeNames != null && attributeNames.contains("dn")
                    && attributeDefinitions.isEmpty()
                    && "(objectclass=*)".equals(filter.toString().toLowerCase());

            log.debug("Search DNs only: "+dnOnly);

            dns.addListener(new PipelineAdapter() {
                public void objectAdded(PipelineEvent event) {
                    try {
                        EntryData data = (EntryData)event.getObject();
                        String dn = data.getDn();

                        if (dnOnly) {
                            log.debug("Returning DN only.");

                            AttributeValues sv = data.getMergedValues();
                            Entry entry = new Entry(dn, entryMapping, null, sv);

                            results.add(entry);
                            return;
                        }

                        if (unique && effectiveSources.size() == 1 && !data.getMergedValues().isEmpty()) {
                            log.debug("Entry data is complete, returning entry.");

                            AttributeValues sv = data.getMergedValues();
                            AttributeValues attributeValues = computeAttributeValues(entryMapping, sv, interpreter);

                            Entry entry = new Entry(dn, entryMapping, attributeValues, sv);

                            log.debug("Checking filter "+filter+" on "+entry.getDn());
                            if (handler.getFilterTool().isValid(entry, filter)) {
                                results.add(entry);
                            } else {
                                log.debug("Entry \""+entry.getDn()+"\" doesn't match search filter.");
                            }

                            return;
                        }

                        log.debug("Entry data is incomplete, loading full entry data.");
                        entriesToLoad.add(data);

                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }

                public void pipelineClosed(PipelineEvent event) {
                    int rc = dns.getReturnCode();
                    //log.debug("RC: "+rc);

                    if (dnOnly) {
                        results.setReturnCode(rc);
                    } else {
                        entriesToLoad.setReturnCode(rc);
                        entriesToLoad.close();
                    }
                }
            });

            log.debug("Filter cache for "+filter+" not found.");

            dns.addListener(new PipelineAdapter() {
                public void objectAdded(PipelineEvent event) {
                    try {
                        EntryData data = (EntryData)event.getObject();
                        String dn = data.getDn();

                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            });

            searchEngine.search(partition, sourceValues, entryMapping, filter, dns);

            dns.close();

            if (dnOnly) return rc;
            //if (unique && effectiveSources.size() == 1) return LDAPException.SUCCESS;

            load(partition, entryMapping, entriesToLoad, loadedEntries);

            newEntries.addListener(new PipelineAdapter() {
                public void objectAdded(PipelineEvent event) {
                    try {
                        Entry entry = (Entry)event.getObject();

                        log.debug("Checking filter "+filter+" on "+entry.getDn());
                        if (handler.getFilterTool().isValid(entry, filter)) {
                            results.add(entry);
                        } else {
                            log.debug("Entry \""+entry.getDn()+"\" doesn't match search filter.");
                        }

                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }

                public void pipelineClosed(PipelineEvent event) {
                    int rc = newEntries.getReturnCode();
                    //log.debug("RC: "+rc);

                    results.setReturnCode(rc);
                }
            });

            merge(partition, entryMapping, loadedEntries, newEntries);

            return rc;

        } finally {
            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("EXPAND MAPPING", 80));
                log.debug(Formatter.displayLine("Return code: "+rc, 80));
                log.debug(Formatter.displaySeparator(80));
            }
        }
    }
}

