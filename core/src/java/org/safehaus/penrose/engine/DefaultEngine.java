/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPConnection;

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

            if (attributeName.equals("entryCSN"))
                continue; // ignore
            if (attributeName.equals("modifiersName"))
                continue; // ignore
            if (attributeName.equals("modifyTimestamp"))
                continue; // ignore

            if (attributeName.equals("objectClass"))
                return LDAPException.OBJECT_CLASS_MODS_PROHIBITED;

            // check if the attribute is defined in the object class

            boolean found = false;
            for (Iterator j = objectClasses.iterator(); j.hasNext();) {
                ObjectClass oc = (ObjectClass) j.next();
                //log.debug("Object Class: " + oc.getName());
                //log.debug(" - required: " + oc.getRequiredAttributes());
                //log.debug(" - optional: " + oc.getOptionalAttributes());

                if (oc.containsRequiredAttribute(attributeName) || oc.containsOptionalAttribute(attributeName)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                log.debug("Can't find attribute " + attributeName
                        + " in object classes "+objectClasses);
                return LDAPException.OBJECT_CLASS_VIOLATION;
            }

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

        Entry newEntry = new Entry(entry.getDn(), entryMapping, entry.getSourceValues(), newValues);

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

        threadManager.execute(new RefreshThread(this));

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

    public int expand(
            PenroseSession session,
            final Partition partition,
            final Entry baseEntry,
            final AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            final String baseDn,
            final Filter filter,
            final PenroseSearchControls sc,
            final PenroseSearchResults results
    ) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("EXPAND MAPPING", 80));
            log.debug(Formatter.displayLine("Entry: \""+entryMapping.getDn()+"\"", 80));
            log.debug(Formatter.displayLine("Base DN: "+baseDn, 80));
            log.debug(Formatter.displayLine("Filter: "+filter, 80));
            log.debug(Formatter.displayLine("Scope: "+LDAPUtil.getScope(sc.getScope()), 80));
            log.debug(Formatter.displayLine("Parent source values:", 80));

            if (parentSourceValues != null) {
                for (Iterator i = parentSourceValues.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Collection values = parentSourceValues.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                }
            }

            log.debug(Formatter.displaySeparator(80));
        }

        if (!getFilterTool().isValid(entryMapping, filter)) {
            return LDAPException.SUCCESS;

        } if (sc.getScope() == PenroseSearchControls.SCOPE_BASE) {
            if (!(EntryUtil.match(entryMapping.getDn(), baseDn))) {
                return LDAPException.SUCCESS;
            }

        } else if (sc.getScope() == PenroseSearchControls.SCOPE_ONE) {
            if (!EntryUtil.match(entryMapping.getParentDn(), baseDn)) {
                return LDAPException.SUCCESS;
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
                return LDAPException.SUCCESS;
            }
        }

        final boolean unique = isUnique(partition, entryMapping);
        final Collection effectiveSources = partition.getEffectiveSourceMappings(entryMapping);

        final PenroseSearchResults dns = new PenroseSearchResults();
        final PenroseSearchResults entriesToLoad = new PenroseSearchResults();
        final PenroseSearchResults loadedEntries = new PenroseSearchResults();
        final PenroseSearchResults newEntries = new PenroseSearchResults();

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
                        Entry entry = new Entry(dn, entryMapping, sv, null);

                        results.add(entry);
                        return;
                    }

                    if (unique && effectiveSources.size() == 1 && !data.getMergedValues().isEmpty()) {
                        log.debug("Entry data is complete, returning entry.");

                        AttributeValues sv = data.getMergedValues();
                        AttributeValues attributeValues = computeAttributeValues(entryMapping, sv, interpreter);

                        Entry entry = new Entry(dn, entryMapping, sv, attributeValues);
                        results.add(entry);

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

        searchEngine.search(partition, baseEntry, parentSourceValues, entryMapping, filter, dns);

        dns.close();

        if (dnOnly) return LDAPException.SUCCESS;
        //if (unique && effectiveSources.size() == 1) return LDAPException.SUCCESS;

        load(partition, entryMapping, entriesToLoad, loadedEntries);

        newEntries.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    Entry entry = (Entry)event.getObject();
                    results.add(entry);

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

        return LDAPException.SUCCESS;
    }

    public int search(
            PenroseSession session,
            Partition partition,
            AttributeValues parentSourceValues,
            EntryMapping entryMapping,
            Entry baseEntry,
            String baseDn,
            Filter filter,
            PenroseSearchControls sc,
            PenroseSearchResults results
    ) throws Exception {

        try {
            if (sc.getScope() != LDAPConnection.SCOPE_BASE && sc.getScope() != LDAPConnection.SCOPE_SUB) {
                return LDAPException.SUCCESS;
            }

            results.add(baseEntry);

            return LDAPException.SUCCESS;

        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;

        } finally {
            results.close();
        }
    }

    public Entry find(
            Partition partition,
            Entry parent,
            AttributeValues parentSourceValues,
            EntryMapping entryMapping,
            String dn
    ) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("FIND", 80));
            log.debug(Formatter.displayLine("Entry: "+dn, 80));
            log.debug(Formatter.displayLine("Parent source values:", 80));

            if (parentSourceValues != null) {
                for (Iterator i = parentSourceValues.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Collection values = parentSourceValues.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                }
            }

            log.debug(Formatter.displaySeparator(80));
        }

        Row rdn = EntryUtil.getRdn(dn);

        EntryData data = new EntryData();
        data.setDn(dn);
        data.setFilter(rdn);

        Collection list = new ArrayList();
        list.add(data);

        AttributeValues loadedSourceValues = loadEngine.loadEntries(
                partition,
                parentSourceValues,
                entryMapping,
                list
        );

        if (loadedSourceValues == null) return null;
        
        final Interpreter interpreter = getInterpreterManager().newInstance();

        SourceMapping primarySourceMapping = getPrimarySource(entryMapping);

        Row filter = createFilter(partition, interpreter, primarySourceMapping, entryMapping, rdn);

        Entry entry = mergeEngine.mergeEntries(
                partition,
                dn,
                entryMapping,
                parentSourceValues,
                loadedSourceValues,
                new ArrayList(),
                interpreter,
                filter
        );

/*
        PenroseSearchResults results = new PenroseSearchResults();

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_BASE);

        Filter filter = FilterTool.createFilter(rdn);

        expand(
                session,
                partition,
                parentPath,
                parentSourceValues,
                entryMapping,
                dn,
                filter,
                sc,
                results
        );

        results.close();

        Entry entry = results.hasNext() ? (Entry)results.next() : null;
*/
        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("FIND RESULT", 80));

            log.debug(Formatter.displayLine("Path:", 80));
            if (entry != null) {
                log.debug(Formatter.displayLine(" - "+entry.getDn(), 80));
            }

            log.debug(Formatter.displaySeparator(80));
        }

        return entry;
    }
}

