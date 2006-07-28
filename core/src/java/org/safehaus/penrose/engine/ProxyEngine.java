package org.safehaus.penrose.engine;

import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.connector.Connection;
import org.safehaus.penrose.ldap.LDAPAdapter;
import org.safehaus.penrose.ldap.LDAPClient;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.util.*;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPConnection;

import javax.naming.directory.*;
import javax.naming.NamingEnumeration;
import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ProxyEngine extends Engine {

    public final static String DEFAULT_CACHE_CLASS = EntryCache.class.getName();

    public void init() throws Exception {
        super.init();

        CacheConfig cacheConfig = penroseConfig.getEntryCacheConfig();
        String cacheClass = cacheConfig.getCacheClass() == null ? ProxyEngine.DEFAULT_CACHE_CLASS : cacheConfig.getCacheClass();

        log.debug("Initializing entry cache "+cacheClass);
        Class clazz = Class.forName(cacheClass);
        entryCache = (EntryCache)clazz.newInstance();

        entryCache.setCacheConfig(cacheConfig);
        entryCache.setPenroseConfig(penroseConfig);
        entryCache.setConnectionManager(connectionManager);
        entryCache.setPartitionManager(partitionManager);
        entryCache.setThreadManager(threadManager);

        entryCache.init();

        engineFilterTool      = new EngineFilterTool(this);
        loadEngine      = new LoadEngine(this);
        mergeEngine     = new MergeEngine(this);
        joinEngine      = new JoinEngine(this);
        transformEngine = new TransformEngine(this);

        log.debug("Proxy engine initialized.");
    }

    public int bind(
            Partition partition,
            Entry entry,
            String password
            ) throws Exception {

        EntryMapping entryMapping = entry.getEntryMapping();
        String dn = entry.getDn();
        String proxyDn = entryMapping.getDn();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);
        LDAPAdapter adapter = (LDAPAdapter)connection.getAdapter();

        String targetDn = dn.substring(0, dn.length() - proxyDn.length());
        if (targetDn.endsWith(",")) targetDn = targetDn.substring(0, targetDn.length()-1);

        String baseDn = sourceConfig.getParameter(LDAPAdapter.BASE_DN);
        targetDn = EntryUtil.append(targetDn, baseDn);

        log.debug("Binding via proxy "+sourceName+" as \""+targetDn+"\" with "+password);

        LDAPClient client = adapter.getClient();
        return client.bind(targetDn, password);
    }

    public int add(
            Partition partition,
            Entry parent,
            EntryMapping entryMapping,
            String dn,
            Attributes attributes
            ) throws Exception {

        EntryMapping proxyMapping = parent.getEntryMapping();

        SourceMapping sourceMapping = proxyMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);
        LDAPAdapter adapter = (LDAPAdapter)connection.getAdapter();

        String proxyDn = proxyMapping.getDn();

        String targetDn = dn.substring(0, dn.length() - proxyDn.length());
        if (targetDn.endsWith(",")) targetDn = targetDn.substring(0, targetDn.length()-1);

        String baseDn = sourceConfig.getParameter(LDAPAdapter.BASE_DN);
        targetDn = EntryUtil.append(targetDn, baseDn);

        log.debug("Modifying via proxy "+sourceName+" as \""+targetDn+"\"");

        LDAPClient client = adapter.getClient();
        return client.add(targetDn, attributes);
    }

    public int modify(
            Partition partition,
            Entry entry,
            Collection modifications
            ) throws Exception {

        EntryMapping entryMapping = entry.getEntryMapping();
        String dn = entry.getDn();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);
        LDAPAdapter adapter = (LDAPAdapter)connection.getAdapter();

        String proxyDn = entryMapping.getDn();

        String targetDn = dn.substring(0, dn.length() - proxyDn.length());
        if (targetDn.endsWith(",")) targetDn = targetDn.substring(0, targetDn.length()-1);

        String baseDn = sourceConfig.getParameter(LDAPAdapter.BASE_DN);
        targetDn = EntryUtil.append(targetDn, baseDn);

        log.debug("Modifying via proxy "+sourceName+" as \""+targetDn+"\"");

        LDAPClient client = adapter.getClient();
        return client.modify(targetDn, modifications);
    }

    public int modrdn(
            Partition partition,
            Entry entry,
            String newRdn
            ) throws Exception {

        EntryMapping entryMapping = entry.getEntryMapping();
        String dn = entry.getDn();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);
        LDAPAdapter adapter = (LDAPAdapter)connection.getAdapter();

        String proxyDn = entryMapping.getDn();

        String targetDn = dn.substring(0, dn.length() - proxyDn.length());
        if (targetDn.endsWith(",")) targetDn = targetDn.substring(0, targetDn.length()-1);

        String baseDn = sourceConfig.getParameter(LDAPAdapter.BASE_DN);
        targetDn = EntryUtil.append(targetDn, baseDn);

        log.debug("Renaming via proxy "+sourceName+" as \""+targetDn+"\"");

        LDAPClient client = adapter.getClient();
        return client.modrdn(targetDn, newRdn);
    }

    public int delete(
            Partition partition,
            Entry entry
            ) throws Exception {

        EntryMapping entryMapping = entry.getEntryMapping();
        String dn = entry.getDn();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);
        LDAPAdapter adapter = (LDAPAdapter)connection.getAdapter();

        String proxyDn = entryMapping.getDn();

        String targetDn = dn.substring(0, dn.length() - proxyDn.length());
        if (targetDn.endsWith(",")) targetDn = targetDn.substring(0, targetDn.length()-1);

        String baseDn = sourceConfig.getParameter(LDAPAdapter.BASE_DN);
        targetDn = EntryUtil.append(targetDn, baseDn);

        log.debug("Modifying via proxy "+sourceName+" as \""+targetDn+"\"");

        LDAPClient client = adapter.getClient();
        return client.delete(targetDn);
    }

    public int search(
            final Partition partition,
            Collection path,
            AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            final String baseDn,
            final Filter filter,
            PenroseSearchControls sc,
            final PenroseSearchResults results
    ) throws Exception {

        if (sc.getScope() == LDAPConnection.SCOPE_BASE) {

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

        } else {

            List parentPath = new ArrayList();
            parentPath.addAll(path);
            parentPath.remove(parentPath.size() - 1);

            expand(partition, parentPath, parentSourceValues, entryMapping, baseDn, filter, sc, results);
        }

        results.close();

        return LDAPException.SUCCESS;
    }

    public int expand(
            final Partition partition,
            Collection parentPath,
            AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            final String baseDn,
            final Filter filter,
            PenroseSearchControls sc,
            final PenroseSearchResults results
            ) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("EXPAND PROXY", 80));
            log.debug(Formatter.displayLine("Entry: \""+entryMapping.getDn()+"\"", 80));
            log.debug(Formatter.displayLine("Base DN: "+baseDn, 80));
            log.debug(Formatter.displayLine("Filter: "+filter, 80));
            log.debug(Formatter.displayLine("Scope: "+LDAPUtil.getScope(sc.getScope()), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);
        LDAPAdapter adapter = (LDAPAdapter)connection.getAdapter();

        final String proxyDn = entryMapping.getDn();
        log.debug("Proxy DN: "+proxyDn);

        final String proxyBaseDn = sourceConfig.getParameter(LDAPAdapter.BASE_DN);
        log.debug("Proxy Base DN: "+proxyBaseDn);

/*
Mapping: ou=Groups,dc=Proxy,dc=Example,dc=org
 - dc=Proxy,dc=Example,dc=org (base) => false
 - dc=Proxy,dc=Example,dc=org (one) => ou=Groups,dc=Proxy,dc=Example,dc=org (base)
 - dc=Proxy,dc=Example,dc=org (sub) => ou=Groups,dc=Proxy,dc=Example,dc=org (sub)
 - ou=Groups,dc=Proxy,dc=Example,dc=org (base) => ou=Groups,dc=Proxy,dc=Example,dc=org (base)
 - ou=Groups,dc=Proxy,dc=Example,dc=org (one) => ou=Groups,dc=Proxy,dc=Example,dc=org (one)
 - ou=Groups,dc=Proxy,dc=Example,dc=org (sub) => ou=Groups,dc=Proxy,dc=Example,dc=org (sub)
 - cn=PD Managers,ou=Groups,dc=Proxy,dc=Example,dc=org (base) => cn=PD Managers,ou=Groups,dc=Proxy,dc=Example,dc=org (base)
 - cn=PD Managers,ou=Groups,dc=Proxy,dc=Example,dc=org (one) => cn=PD Managers,ou=Groups,dc=Proxy,dc=Example,dc=org (one)
 - cn=PD Managers,ou=Groups,dc=Proxy,dc=Example,dc=org (sub) => cn=PD Managers,ou=Groups,dc=Proxy,dc=Example,dc=org (sub)
*/
        log.debug("Checking whether "+entryMapping.getDn()+" is an ancestor of "+baseDn);
        String dn = baseDn;
        boolean found = false;
        while (dn != null) {
            if (EntryUtil.match(dn, entryMapping.getDn())) {
                found = true;
                break;
            }
            dn = EntryUtil.getParentDn(dn);
        }

        log.debug("Result: "+found);

        PenroseSearchControls newSc = new PenroseSearchControls();
        newSc.setAttributes(sc.getAttributes());
        newSc.setScope(sc.getScope());

        String targetDn = "";
        if (found) {
            targetDn = baseDn.substring(0, baseDn.length() - entryMapping.getDn().length());
            if (targetDn.endsWith(",")) targetDn = targetDn.substring(0, targetDn.length()-1);

        } else {
            if (sc.getScope() == LDAPConnection.SCOPE_BASE) {
                return LDAPException.SUCCESS;

            } else if (sc.getScope() == LDAPConnection.SCOPE_ONE) {
                newSc.setScope(LDAPConnection.SCOPE_BASE);
            }
        }
        log.debug("Target DN: "+targetDn);

        targetDn = EntryUtil.append(targetDn, proxyBaseDn);

        log.debug("Searching proxy "+sourceName+" for \""+targetDn+"\" with filter="+filter+" attrs="+newSc.getAttributes());

        final LDAPClient client = adapter.getClient();

        try {
            PenroseSearchResults res = new PenroseSearchResults();

            res.addListener(new PipelineAdapter() {
                public void objectAdded(PipelineEvent event) {
                    try {
                        SearchResult ldapEntry = (SearchResult)event.getObject();
                        //log.debug("Subtracting \""+proxyBaseDn+"\" from \""+ldapEntry.getDN()+"\"");

                        String dn = ldapEntry.getName();

                        if (proxyBaseDn != null) {
                            dn = dn.substring(0, ldapEntry.getName().length() - proxyBaseDn.length());
                            if (dn.endsWith(",")) dn = dn.substring(0, dn.length()-1);
                        }

                        //dn = "".equals(ldapEntry.getDN()) ? baseDn : ldapEntry.getDN()+","+baseDn;
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
            });

            client.search(targetDn, filter.toString(), newSc, res);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            results.setReturnCode(ExceptionUtil.getReturnCode(e));
        }

        return LDAPException.SUCCESS;
    }

    public void start() throws Exception {
        super.start();

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
        super.stop();
    }
}
