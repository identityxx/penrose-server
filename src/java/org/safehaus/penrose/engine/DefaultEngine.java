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
import org.safehaus.penrose.connector.*;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.util.Formatter;
import org.ietf.ldap.LDAPException;

import javax.naming.directory.Attributes;
import javax.naming.directory.SearchResult;
import javax.naming.directory.Attribute;
import javax.naming.NamingEnumeration;
import javax.naming.Context;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class DefaultEngine extends Engine {

    public final static String PROXY_BASE_DN                = "baseDn";
    public final static String PROXY_AUTHENTICATON          = "authentication";
    public final static String PROXY_AUTHENTICATON_DEFAULT  = "default";
    public final static String PROXY_AUTHENTICATON_FULL     = "full";
    public final static String PROXY_AUTHENTICATON_DISABLED = "disabled";

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
        entryCache.setPenroseConfig(penroseConfig);
        entryCache.setConnectionManager(connectionManager);
        entryCache.setPartitionManager(partitionManager);
        entryCache.setThreadManager(threadManager);

        entryCache.init();

        log.debug("Initializing engine...");

        filterTool      = new EngineFilterTool(this);
        addEngine       = new AddEngine(this);
        deleteEngine    = new DeleteEngine(this);
        modifyEngine    = new ModifyEngine(this);
        modrdnEngine    = new ModRdnEngine(this);
        searchEngine    = new SearchEngine(this);
        loadEngine      = new LoadEngine(this);
        mergeEngine     = new MergeEngine(this);
        joinEngine      = new JoinEngine(this);
        transformEngine = new TransformEngine(this);
    }

    public int bind(Entry entry, String password) throws Exception {

        log.debug("Bind as user "+entry.getDn()+" with password "+password);

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
        Partition partition = partitionManager.getPartition(entryMapping);

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            SourceMapping source = (SourceMapping)i.next();

            SourceConfig sourceConfig = partition.getSourceConfig(source.getSourceName());

            Map entries = transformEngine.split(entryMapping, source, attributeValues);

            for (Iterator j=entries.keySet().iterator(); j.hasNext(); ) {
                Row pk = (Row)j.next();
                //AttributeValues sourceValues = (AttributeValues)entries.get(pk);

                log.debug("Bind to "+source.getName()+" as "+pk+".");

                int rc = connector.bind(partition, sourceConfig, entryMapping, pk, password);
                if (rc == LDAPException.SUCCESS) return rc;
            }
        }

        return LDAPException.INVALID_CREDENTIALS;
    }

    public int add(
            Entry parent,
            EntryMapping entryMapping,
            AttributeValues attributeValues)
            throws Exception {

        // normalize attribute names
        AttributeValues newAttributeValues = new AttributeValues();
        for (Iterator i = attributeValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            if ("objectClass".equalsIgnoreCase(name)) continue;

            AttributeMapping attributeMapping = entryMapping.getAttributeMapping(name);
            if (attributeMapping == null) {
                log.debug("Undefined attribute "+name);
                return LDAPException.OBJECT_CLASS_VIOLATION;
            }

            Collection values = attributeValues.get(name);
            newAttributeValues.set(attributeMapping.getName(), values);
        }

        attributeValues = newAttributeValues;

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

        int rc = addEngine.add(parent, entryMapping, attributeValues);

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ADD RC:"+rc, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        return rc;
    }

    public int delete(Entry entry) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("DELETE", 80));
            log.debug(Formatter.displayLine("DN: "+entry.getDn(), 80));

            log.debug(Formatter.displaySeparator(80));
        }

        int rc = deleteEngine.delete(entry);

        return rc;
    }

    public int modrdn(Entry entry, String newRdn, boolean deleteOldRdn) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODRDN", 80));
            log.debug(Formatter.displayLine("DN: "+entry.getDn(), 80));
            log.debug(Formatter.displayLine("New RDN: "+newRdn, 80));
        }

        int rc = modrdnEngine.modrdn(entry, newRdn, deleteOldRdn);

        return rc;
    }

    public int modify(Entry entry, AttributeValues newValues) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODIFY", 80));
            log.debug(Formatter.displayLine("DN: "+entry.getDn(), 80));

            log.debug(Formatter.displayLine("Old attribute values:", 80));
            AttributeValues oldValues = entry.getAttributeValues();
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

        int rc = modifyEngine.modify(entry, newValues);

        return rc;
    }

    public void search(
            Entry parent,
            AttributeValues parentSourceValues,
            EntryMapping entryMapping,
            Filter filter,
            PenroseSearchResults dns)
            throws Exception {

        searchEngine.search(parent, parentSourceValues, entryMapping, filter, dns);
    }

    public String convertDn(String dn, String oldSuffix, String newSuffix) throws Exception {

        if (dn == null) return null;
        if (!dn.endsWith(oldSuffix)) return null;

        dn = dn.substring(0, dn.length() - oldSuffix.length());
        if (dn.endsWith(",")) dn = dn.substring(0, dn.length()-1);

        return EntryUtil.append(dn, newSuffix);
    }

    public int bindProxy(
            PenroseSession session,
            Partition partition,
            EntryMapping entryMapping,
            String dn,
            String password
            ) throws Exception {

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);
        if (PROXY_AUTHENTICATON_DISABLED.equals(pta)) {
            log.debug("Pass-Through Authentication is disabled.");
            return LDAPException.INVALID_CREDENTIALS;
        }

        Connection connection = connectionManager.getConnection(partition, connectionName);

        String proxyDn = entryMapping.getDn();
        String baseDn = sourceConfig.getParameter(PROXY_BASE_DN);

        String bindDn = convertDn(dn, proxyDn, baseDn);

        log.debug("Binding via proxy "+sourceName+" as \""+bindDn +"\" with "+password);

        Map parameters = new HashMap();
        parameters.putAll(connection.getParameters());

        JNDIClient client = new JNDIClient(parameters);

        try {
            client.bind(bindDn, password);

            return LDAPException.SUCCESS;

        } catch (Exception e) {
            return ExceptionUtil.getReturnCode(e);

        } finally {
            if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
                log.debug("Storing connection info in session.");
                session.setAttribute(partition.getName()+"."+sourceName+".connection", client);
            } else {
                client.close();
            }
        }
    }

    public int addProxy(
            PenroseSession session,
            Partition partition,
            EntryMapping entryMapping,
            String dn,
            Attributes attributes
            ) throws Exception {

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);

        String proxyDn = entryMapping.getDn();
        String baseDn = sourceConfig.getParameter(PROXY_BASE_DN);

        String targetDn = convertDn(dn, proxyDn, baseDn);

        log.debug("Modifying via proxy "+sourceName+" as \""+targetDn+"\"");

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);

        JNDIClient client;

        if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
            log.debug("Getting connection info from session.");
            client = (JNDIClient)session.getAttribute(partition.getName()+"."+sourceName+".connection");
            if (client == null) {
                log.debug("Missing credentials.");
                return LDAPException.SUCCESS;
            }
        } else {
            log.debug("Creating new connection.");
            Map parameters = new HashMap();
            parameters.putAll(connection.getParameters());

            client = new JNDIClient(parameters);
        }

        try {
            client.add(targetDn, attributes);

            return LDAPException.SUCCESS;

        } catch (Exception e) {
            return ExceptionUtil.getReturnCode(e);

        } finally {
            if (!PROXY_AUTHENTICATON_FULL.equals(pta)) {
                client.close();
            }
        }
    }

    public int modifyProxy(
            PenroseSession session,
            Partition partition,
            EntryMapping entryMapping,
            Entry entry,
            Collection modifications
            ) throws Exception {

        String dn = entry.getDn();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);

        String proxyDn = entryMapping.getDn();
        String baseDn = sourceConfig.getParameter(PROXY_BASE_DN);

        String targetDn = convertDn(dn, proxyDn, baseDn);

        log.debug("Modifying via proxy "+sourceName+" as \""+targetDn+"\"");

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);

        JNDIClient client;

        if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
            log.debug("Getting connection info from session.");
            client = (JNDIClient)session.getAttribute(partition.getName()+"."+sourceName+".connection");
            if (client == null) {
                log.debug("Missing credentials.");
                return LDAPException.SUCCESS;
            }
        } else {
            log.debug("Creating new connection.");
            Map parameters = new HashMap();
            parameters.putAll(connection.getParameters());

            client = new JNDIClient(parameters);
        }

        try {
            client.modify(targetDn, modifications);

            return LDAPException.SUCCESS;

        } catch (Exception e) {
            return ExceptionUtil.getReturnCode(e);

        } finally {
            if (!PROXY_AUTHENTICATON_FULL.equals(pta)) {
                client.close();
            }
        }
    }

    public int modrdnProxy(
            PenroseSession session,
            Partition partition,
            EntryMapping entryMapping,
            Entry entry,
            String newRdn, boolean deleteOldRdn) throws Exception {

        String dn = entry.getDn();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);

        String proxyDn = entryMapping.getDn();
        String baseDn = sourceConfig.getParameter(PROXY_BASE_DN);

        String targetDn = convertDn(dn, proxyDn, baseDn);

        log.debug("Renaming via proxy "+sourceName+" as \""+targetDn+"\"");

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);

        JNDIClient client;

        if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
            log.debug("Getting connection info from session.");
            client = (JNDIClient)session.getAttribute(partition.getName()+"."+sourceName+".connection");
            if (client == null) {
                log.debug("Missing credentials.");
                return LDAPException.SUCCESS;
            }
        } else {
            log.debug("Creating new connection.");
            Map parameters = new HashMap();
            parameters.putAll(connection.getParameters());

            client = new JNDIClient(parameters);
        }

        try {
            client.modrdn(targetDn, newRdn);

            return LDAPException.SUCCESS;

        } catch (Exception e) {
            return ExceptionUtil.getReturnCode(e);

        } finally {
            if (!PROXY_AUTHENTICATON_FULL.equals(pta)) {
                client.close();
            }
        }
    }

    public int deleteProxy(
            PenroseSession session,
            Partition partition,
            EntryMapping entryMapping,
            String dn
            ) throws Exception {

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);

        String proxyDn = entryMapping.getDn();
        String baseDn = sourceConfig.getParameter(PROXY_BASE_DN);

        String targetDn = convertDn(dn, proxyDn, baseDn);

        log.debug("Modifying via proxy "+sourceName+" as \""+targetDn+"\"");

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);

        JNDIClient client;

        if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
            log.debug("Getting connection info from session.");
            client = (JNDIClient)session.getAttribute(partition.getName()+"."+sourceName+".connection");
            if (client == null) {
                log.debug("Missing credentials.");
                return LDAPException.SUCCESS;
            }
        } else {
            log.debug("Creating new connection.");
            Map parameters = new HashMap();
            parameters.putAll(connection.getParameters());

            client = new JNDIClient(parameters);
        }

        try {
            client.delete(targetDn);

            return LDAPException.SUCCESS;

        } catch (Exception e) {
            return ExceptionUtil.getReturnCode(e);

        } finally {
            if (!PROXY_AUTHENTICATON_FULL.equals(pta)) {
                client.close();
            }
        }
    }

    public int searchProxy(
            final PenroseSession session,
            final Partition partition,
            final EntryMapping entryMapping,
            final String base,
            final String filter,
            PenroseSearchControls sc,
            final PenroseSearchResults results
            ) throws Exception {

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);

        final String proxyDn = entryMapping.getDn();
        final String baseDn = sourceConfig.getParameter(PROXY_BASE_DN);

        String targetDn = convertDn(base, proxyDn, baseDn);
        String bindDn = convertDn(session.getBindDn(), proxyDn, baseDn);

        log.debug("Searching proxy "+sourceName+" for \""+targetDn+"\" with filter="+filter+" attrs="+sc.getAttributes());
        log.debug("Bind DN: "+bindDn);

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);

        JNDIClient client;

        if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
            log.debug("Getting connection info from session.");
            client = (JNDIClient)session.getAttribute(partition.getName()+"."+sourceName+".connection");
            if (client == null) {
                log.debug("Missing credentials.");
                results.close();
                return LDAPException.SUCCESS;
            }
        } else {
            log.debug("Creating new connection.");
            Map parameters = new HashMap();
            parameters.putAll(connection.getParameters());

            client = new JNDIClient(parameters);
        }

        PenroseSearchResults res = new PenroseSearchResults();

        res.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    SearchResult ldapEntry = (SearchResult)event.getObject();
                    //log.debug("Subtracting \""+baseDn+"\" from \""+ldapEntry.getDN()+"\"");

                    String dn = ldapEntry.getName();

                    if (baseDn != null) {
                        dn = dn.substring(0, ldapEntry.getName().length() - baseDn.length());
                        if (dn.endsWith(",")) dn = dn.substring(0, dn.length()-1);
                    }

                    //dn = "".equals(ldapEntry.getDN()) ? base : ldapEntry.getDN()+","+base;
                    dn = EntryUtil.append(dn, proxyDn);
                    //log.debug("=> "+dn);

                    AttributeValues attributeValues = new AttributeValues();

                    //log.debug("Entry "+dn+":");
                    for (NamingEnumeration i=ldapEntry.getAttributes().getAll(); i.hasMore(); ) {
                        Attribute attribute = (Attribute)i.next();
                        String name = attribute.getID();

                        for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                            Object value = j.next();
                            attributeValues.add(name, value);
                            //log.debug(" - "+name+": "+value);
                        }
                    }

                    Entry entry = new Entry(dn, entryMapping, attributeValues);
                    results.add(entry);

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

            public void pipelineClosed(PipelineEvent event) {
                results.close();
            }
        });

        try {
            client.search(targetDn, filter, sc, res);

            return LDAPException.SUCCESS;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            int rc = ExceptionUtil.getReturnCode(e);
            results.setReturnCode(rc);
            return rc;

        } finally {
            if (!PROXY_AUTHENTICATON_FULL.equals(pta)) {
                client.close();
            }
        }
    }

    public SearchEngine getSearchEngine() {
        return searchEngine;
    }

    public void setSearchEngine(SearchEngine searchEngine) {
        this.searchEngine = searchEngine;
    }

    public void start() throws Exception {

        //log.debug("Starting Engine...");

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();

            for (Iterator j=partition.getRootEntryMappings().iterator(); j.hasNext(); ) {
                EntryMapping entryMapping = (EntryMapping)j.next();
                analyzer.analyze(entryMapping);
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
    }

    public void search(
            final Collection path,
            final AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            boolean single,
            final Filter filter,
            PenroseSearchControls sc,
            final PenroseSearchResults results) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH", 80));
            log.debug(Formatter.displayLine("Entry: \""+entryMapping.getDn()+"\"", 80));
            log.debug(Formatter.displayLine("Filter: "+filter, 80));
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

        final PenroseSearchResults dns = new PenroseSearchResults();
        final PenroseSearchResults entriesToLoad = new PenroseSearchResults();
        final PenroseSearchResults loadedEntries = new PenroseSearchResults();
        final PenroseSearchResults newEntries = new PenroseSearchResults();

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
                    EntryData map = (EntryData)event.getObject();
                    String dn = map.getDn();

                    Entry entry = getEntryCache().get(dn);
                    log.debug("Entry cache for "+dn+": "+(entry == null ? "not found." : "found."));

                    if (entry == null) {

                        if (dnOnly) {
                            AttributeValues sv = map.getMergedValues();
                            //AttributeValues attributeValues = handler.getEngine().computeAttributeValues(entryMapping, sv, interpreter);
                            entry = new Entry(dn, entryMapping, sv, null);

                            results.add(entry);

                        } else {
                            entriesToLoad.add(map);
                        }

                    } else {
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
                    results.close();
                } else {
                    entriesToLoad.setReturnCode(rc);
                    entriesToLoad.close();
                }
            }
        });

        Entry parent = null;
        if (path != null && path.size() > 0) {
            parent = (Entry)path.iterator().next();
        }

        log.debug("Parent: "+(parent == null ? null : parent.getDn()));
        String parentDn = parent == null ? null : parent.getDn();

        boolean cacheFilter = getEntryCache().contains(entryMapping, parentDn, filter);

        if (!cacheFilter) {

            log.debug("Filter cache for "+filter+" not found.");

            dns.addListener(new PipelineAdapter() {
                public void objectAdded(PipelineEvent event) {
                    try {
                        EntryData map = (EntryData)event.getObject();
                        String dn = map.getDn();

                        log.info("Storing "+dn+" in filter cache.");

                        getEntryCache().add(entryMapping, filter, dn);

                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            });

            search(parent, parentSourceValues, entryMapping, filter, dns);

        } else {
            log.debug("Filter cache for "+filter+" found.");

            PenroseSearchResults list = new PenroseSearchResults();

            list.addListener(new PipelineAdapter() {
                public void objectAdded(PipelineEvent event) {
                    try {
                        String dn = (String)event.getObject();
                        log.info("Loading "+dn+" from filter cache.");

                        EntryData map = new EntryData();
                        map.setDn(dn);
                        map.setMergedValues(new AttributeValues());
                        map.setRows(new ArrayList());
                        dns.add(map);

                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }

                public void pipelineClosed(PipelineEvent event) {
                    dns.close();
                }
            });

            getEntryCache().search(entryMapping, parentDn, filter, list);
        }

        if (dnOnly) return;

        load(entryMapping, entriesToLoad, loadedEntries);

        newEntries.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    Entry entry = (Entry)event.getObject();

                    log.info("Storing "+entry.getDn()+" in entry cache.");

                    getEntryCache().put(entry);
                    results.add(entry);

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

            public void pipelineClosed(PipelineEvent event) {
                int rc = newEntries.getReturnCode();
                //log.debug("RC: "+rc);

                results.setReturnCode(rc);
                results.close();
            }
        });

        merge(entryMapping, loadedEntries, newEntries);
    }
}

