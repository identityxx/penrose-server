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
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.PasswordUtil;
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

    public final static String DEFAULT_CACHE_CLASS = EntryCache.class.getName();

    AddEngine addEngine;
    DeleteEngine deleteEngine;
    ModifyEngine modifyEngine;
    ModRdnEngine modrdnEngine;
    SearchEngine searchEngine;

    public void init() throws Exception {
        super.init();

        CacheConfig cacheConfig = penroseConfig.getEntryCacheConfig();
        String cacheClass = cacheConfig.getCacheClass() == null ? DEFAULT_CACHE_CLASS : cacheConfig.getCacheClass();

        log.debug("Initializing entry cache "+cacheClass);
        Class clazz = Class.forName(cacheClass);
        entryCache = (EntryCache)clazz.newInstance();

        entryCache.setCacheConfig(cacheConfig);
        entryCache.setConnectionManager(connectionManager);

        entryCache.init();

        engineFilterTool      = new EngineFilterTool(this);
        addEngine       = new AddEngine(this);
        deleteEngine    = new DeleteEngine(this);
        modifyEngine    = new ModifyEngine(this);
        modrdnEngine    = new ModRdnEngine(this);
        searchEngine    = new SearchEngine(this);
        loadEngine      = new LoadEngine(this);
        mergeEngine     = new MergeEngine(this);
        joinEngine      = new JoinEngine(this);
        transformEngine = new TransformEngine(this);

        log.debug("Default engine initialized.");
    }

    public int bind(Partition partition, Entry entry, String password) throws Exception {

        log.debug("Bind as user "+entry.getDn());

        EntryMapping entryMapping = entry.getEntryMapping();
        AttributeValues attributeValues = entry.getAttributeValues();

        Collection set = attributeValues.get("userPassword");

        if (set != null) {
            for (Iterator i = set.iterator(); i.hasNext(); ) {
                Object userPassword = i.next();
                log.debug("userPassword: "+userPassword);
                if (PasswordUtil.comparePassword(password, userPassword)) return LDAPException.SUCCESS;
            }
        }

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
            Partition partition,
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

        int rc = addEngine.add(partition, parent, entryMapping, attributeValues);

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ADD RC:"+rc, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        return rc;
    }

    public int delete(Partition partition, Entry entry) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("DELETE", 80));
            log.debug(Formatter.displayLine("DN: "+entry.getDn(), 80));

            log.debug(Formatter.displaySeparator(80));
        }

        int rc = deleteEngine.delete(partition, entry);

        return rc;
    }

    public int modrdn(Partition partition, Entry entry, String newRdn) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODRDN", 80));
            log.debug(Formatter.displayLine("DN: "+entry.getDn(), 80));
            log.debug(Formatter.displayLine("New RDN: "+newRdn, 80));
        }

        int rc = modrdnEngine.modrdn(partition, entry, newRdn);

        return rc;
    }

    public int modify(Partition partition, Entry entry, Collection modifications) throws Exception {

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

        threadManager.execute(new RefreshThread(this), false);

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
            final Partition partition,
            final Collection parentPath,
            final AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            final String baseDn,
            final Filter filter,
            final PenroseSearchControls sc,
            final PenroseSearchResults results) throws Exception {

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
        }

/*
Mapping: cn=Managers,ou=Groups,dc=Proxy,dc=Example,dc=org
 - dc=Proxy,dc=Example,dc=org (base) => false
 - dc=Proxy,dc=Example,dc=org (one) => false
 - dc=Proxy,dc=Example,dc=org (sub) => true
 - ou=Groups,dc=Proxy,dc=Example,dc=org (base) => false
 - ou=Groups,dc=Proxy,dc=Example,dc=org (one) => true
 - ou=Groups,dc=Proxy,dc=Example,dc=org (sub) => true
 - cn=Managers,ou=Groups,dc=Proxy,dc=Example,dc=org (base) => true
 - cn=Managers,ou=Groups,dc=Proxy,dc=Example,dc=org (one) => false
 - cn=Managers,ou=Groups,dc=Proxy,dc=Example,dc=org (sub) => true
 - cn=PD Managers,ou=Groups,dc=Proxy,dc=Example,dc=org (base) => false
 - cn=PD Managers,ou=Groups,dc=Proxy,dc=Example,dc=org (one) => false
 - cn=PD Managers,ou=Groups,dc=Proxy,dc=Example,dc=org (sub) => false
*/
        if (sc.getScope() == PenroseSearchControls.SCOPE_BASE) {
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

                    Entry entry = getEntryCache().get(partition, dn);

                    if (entry == null) {
                        log.debug("Entry "+dn+" is not cached.");

                        if (dnOnly) {
                            log.debug("Returning DN only.");

                            AttributeValues sv = data.getMergedValues();
                            entry = new Entry(dn, entryMapping, sv, null);

                            results.add(entry);

                        } else if (unique && effectiveSources.size() == 1) {
                            log.debug("Entry data is complete, returning entry.");
                            AttributeValues sv = data.getMergedValues();
                            AttributeValues attributeValues = computeAttributeValues(entryMapping, sv, interpreter);
                            entry = new Entry(dn, entryMapping, sv, attributeValues);
                            results.add(entry);

                        } else {
                            log.debug("Entry data is incomplete, loading full entry data.");
                            entriesToLoad.add(data);
                        }

                    } else {
                        log.debug("Entry "+dn+" is cached, returning entry.");
                        results.add(entry);
                    }

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

        Entry parent = null;
        if (parentPath != null && parentPath.size() > 0) {
            parent = (Entry)parentPath.iterator().next();
        }

        log.debug("Parent: "+(parent == null ? null : parent.getDn()));
        String parentDn = parent == null ? null : parent.getDn();

        boolean cacheFilter = getEntryCache().contains(partition, entryMapping, parentDn, filter);

        if (!cacheFilter) {

            log.debug("Filter cache for "+filter+" not found.");

            dns.addListener(new PipelineAdapter() {
                public void objectAdded(PipelineEvent event) {
                    try {
                        EntryData data = (EntryData)event.getObject();
                        String dn = data.getDn();

                        log.info("Storing "+dn+" in filter cache.");

                        getEntryCache().add(partition, entryMapping, filter, dn);

                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            });

            searchEngine.search(partition, parent, parentSourceValues, entryMapping, filter, dns);
            
            dns.close();

        } else {
            log.debug("Filter cache for "+filter+" found.");

            PenroseSearchResults list = new PenroseSearchResults();

            list.addListener(new PipelineAdapter() {
                public void objectAdded(PipelineEvent event) {
                    try {
                        String dn = (String)event.getObject();
                        log.info("Loading "+dn+" from filter cache.");

                        EntryData data = new EntryData();
                        data.setDn(dn);
                        data.setMergedValues(new AttributeValues());
                        data.setRows(new ArrayList());
                        dns.add(data);

                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }

                public void pipelineClosed(PipelineEvent event) {
                    dns.close();
                }
            });

            getEntryCache().search(partition, entryMapping, parentDn, filter, list);
        }

        if (dnOnly) return LDAPException.SUCCESS;
        if (unique && effectiveSources.size() == 1) return LDAPException.SUCCESS;

        load(partition, entryMapping, entriesToLoad, loadedEntries);

        newEntries.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    Entry entry = (Entry)event.getObject();

                    log.info("Storing "+entry.getDn()+" in entry cache.");

                    getEntryCache().put(partition, entry);
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
            Partition partition,
            Collection path,
            AttributeValues parentSourceValues,
            EntryMapping entryMapping,
            String baseDn,
            Filter filter,
            PenroseSearchControls sc,
            PenroseSearchResults results
    ) throws Exception {

        if (sc.getScope() == LDAPConnection.SCOPE_BASE || sc.getScope() == LDAPConnection.SCOPE_SUB) {

            Entry baseEntry = (Entry)path.iterator().next();

            if (getFilterTool().isValid(baseEntry, filter)) {

                Entry e = baseEntry;
                Collection attributeNames = sc.getAttributes();

                if (!attributeNames.isEmpty() && !attributeNames.contains("*")) {
                    AttributeValues av = new AttributeValues();
                    av.add(baseEntry.getAttributeValues());
                    av.retain(attributeNames);
                    e = new Entry(baseEntry.getDn(), entryMapping, baseEntry.getSourceValues(), av);
                }

                results.add(e);
            }
        }

        results.close();

        return LDAPException.SUCCESS;
    }

}

