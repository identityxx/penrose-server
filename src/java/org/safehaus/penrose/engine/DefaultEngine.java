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
import org.safehaus.penrose.connector.*;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.thread.ThreadPool;
import org.safehaus.penrose.util.*;
import org.ietf.ldap.LDAPException;

import javax.naming.directory.Attributes;
import javax.naming.directory.SearchResult;
import javax.naming.directory.Attribute;
import javax.naming.NamingEnumeration;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

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
        entryCache.setPenroseConfig(penroseConfig);
        entryCache.setConnectionManager(connectionManager);
        entryCache.setPartitionManager(partitionManager);
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

        log.debug("Bind as user "+entry.getDn());

        EntryMapping entryMapping = entry.getEntryMapping();
        AttributeValues attributeValues = entry.getAttributeValues();

        Collection set = attributeValues.get("userPassword");

        if (set != null) {
            for (Iterator i = set.iterator(); i.hasNext(); ) {
                String userPassword = (String)i.next();
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
                AttributeValues sourceValues = (AttributeValues)entries.get(pk);

                log.debug("Bind to "+source.getName()+" as "+pk+": "+sourceValues);

                int rc = connector.bind(partition, sourceConfig, entryMapping, sourceValues, password);
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

    public int modrdn(Entry entry, String newRdn) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODRDN", 80));
            log.debug(Formatter.displayLine("DN: "+entry.getDn(), 80));
            log.debug(Formatter.displayLine("New RDN: "+newRdn, 80));
        }

        int rc = modrdnEngine.modrdn(entry, newRdn);

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

    public void bindProxy(
            final Partition partition,
            final EntryMapping entryMapping,
            String dn,
            String password
            ) throws Exception {

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);
        JNDIAdapter adapter = (JNDIAdapter)connection.getAdapter();

        String proxyDn = entryMapping.getDn();

        String targetDn = dn.substring(0, dn.length() - proxyDn.length());
        if (targetDn.endsWith(",")) targetDn = targetDn.substring(0, targetDn.length()-1);

        String baseDn = sourceConfig.getParameter("baseDn");
        targetDn = EntryUtil.append(targetDn, baseDn);

        log.debug("Binding via proxy "+sourceName+" as \""+targetDn+"\" with "+password);

        JNDIClient client = adapter.getClient();
        client.bind(targetDn, password);
    }

    public void addProxy(
            final Partition partition,
            final EntryMapping entryMapping,
            String dn,
            Attributes attributes
            ) throws Exception {

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);
        JNDIAdapter adapter = (JNDIAdapter)connection.getAdapter();

        String proxyDn = entryMapping.getDn();

        String targetDn = dn.substring(0, dn.length() - proxyDn.length());
        if (targetDn.endsWith(",")) targetDn = targetDn.substring(0, targetDn.length()-1);

        String baseDn = sourceConfig.getParameter("baseDn");
        targetDn = EntryUtil.append(targetDn, baseDn);

        log.debug("Modifying via proxy "+sourceName+" as \""+targetDn+"\"");

        JNDIClient client = adapter.getClient();
        client.add(targetDn, attributes);
    }

    public void modifyProxy(
            final Partition partition,
            final EntryMapping entryMapping,
            Entry entry,
            Collection modifications
            ) throws Exception {

        String dn = entry.getDn();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);
        JNDIAdapter adapter = (JNDIAdapter)connection.getAdapter();

        String proxyDn = entryMapping.getDn();

        String targetDn = dn.substring(0, dn.length() - proxyDn.length());
        if (targetDn.endsWith(",")) targetDn = targetDn.substring(0, targetDn.length()-1);

        String baseDn = sourceConfig.getParameter("baseDn");
        targetDn = EntryUtil.append(targetDn, baseDn);

        log.debug("Modifying via proxy "+sourceName+" as \""+targetDn+"\"");

        JNDIClient client = adapter.getClient();
        client.modify(targetDn, modifications);
    }

    public void modrdnProxy(
            final Partition partition,
            final EntryMapping entryMapping,
            Entry entry,
            String newRdn
            ) throws Exception {

        String dn = entry.getDn();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);
        JNDIAdapter adapter = (JNDIAdapter)connection.getAdapter();

        String proxyDn = entryMapping.getDn();

        String targetDn = dn.substring(0, dn.length() - proxyDn.length());
        if (targetDn.endsWith(",")) targetDn = targetDn.substring(0, targetDn.length()-1);

        String baseDn = sourceConfig.getParameter("baseDn");
        targetDn = EntryUtil.append(targetDn, baseDn);

        log.debug("Renaming via proxy "+sourceName+" as \""+targetDn+"\"");

        JNDIClient client = adapter.getClient();
        client.modrdn(targetDn, newRdn);
    }

    public void deleteProxy(
            final Partition partition,
            final EntryMapping entryMapping,
            String dn
            ) throws Exception {

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);
        JNDIAdapter adapter = (JNDIAdapter)connection.getAdapter();

        String proxyDn = entryMapping.getDn();

        String targetDn = dn.substring(0, dn.length() - proxyDn.length());
        if (targetDn.endsWith(",")) targetDn = targetDn.substring(0, targetDn.length()-1);

        String baseDn = sourceConfig.getParameter("baseDn");
        targetDn = EntryUtil.append(targetDn, baseDn);

        log.debug("Modifying via proxy "+sourceName+" as \""+targetDn+"\"");

        JNDIClient client = adapter.getClient();
        client.delete(targetDn);
    }

    public void searchProxy(
            final Partition partition,
            final EntryMapping entryMapping,
            final String base,
            final int scope,
            final String filter,
            final Collection attributeNames,
            final PenroseSearchResults results
            ) throws Exception {

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);
        JNDIAdapter adapter = (JNDIAdapter)connection.getAdapter();

        final String proxyDn = entryMapping.getDn();

        String targetDn = base.substring(0, base.length() - proxyDn.length());
        if (targetDn.endsWith(",")) targetDn = targetDn.substring(0, targetDn.length()-1);

        final String baseDn = sourceConfig.getParameter("baseDn");
        targetDn = EntryUtil.append(targetDn, baseDn);

        log.debug("Searching proxy "+sourceName+" for \""+targetDn+"\" with "+filter);

        final JNDIClient client = adapter.getClient();

        try {
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
                        log.debug(e.getMessage(), e);
                    }
                }

                public void pipelineClosed(PipelineEvent event) {
                    results.close();
                }
            });

            client.search(targetDn, scope, filter, attributeNames, res);

        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            results.setReturnCode(ExceptionUtil.getReturnCode(e));
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

        String s = engineConfig.getParameter(EngineConfig.THREAD_POOL_SIZE);
        int threadPoolSize = s == null ? EngineConfig.DEFAULT_THREAD_POOL_SIZE : Integer.parseInt(s);

        threadPool = new ThreadPool(threadPoolSize);

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();

            for (Iterator j=partition.getRootEntryMappings().iterator(); j.hasNext(); ) {
                EntryMapping entryMapping = (EntryMapping)j.next();
                analyzer.analyze(entryMapping);
            }
        }

        threadPool.execute(new RefreshThread(this));

        //log.debug("Engine started.");
    }

    public void stop() throws Exception {
        if (stopping) return;

        log.debug("Stopping Engine...");
        stopping = true;

        // wait for all the worker threads to finish
        if (threadPool != null) threadPool.stopRequestAllWorkers();
        log.debug("Engine stopped.");
    }
}

