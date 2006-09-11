package org.safehaus.penrose.engine;

import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.connector.Connection;
import org.safehaus.penrose.ldap.LDAPClient;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.util.Formatter;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPConnection;

import javax.naming.directory.*;
import javax.naming.NamingEnumeration;
import javax.naming.Context;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ProxyEngine extends Engine {

    public final static String PROXY_BASE_DN                = "baseDn";
    public final static String PROXY_AUTHENTICATON          = "authentication";
    public final static String PROXY_AUTHENTICATON_DEFAULT  = "default";
    public final static String PROXY_AUTHENTICATON_FULL     = "full";
    public final static String PROXY_AUTHENTICATON_DISABLED = "disabled";

    public final static String DEFAULT_CACHE_CLASS = EntryCache.class.getName();

    public void init() throws Exception {
        super.init();

        CacheConfig cacheConfig = penroseConfig.getEntryCacheConfig();
        String cacheClass = cacheConfig.getCacheClass() == null ? ProxyEngine.DEFAULT_CACHE_CLASS : cacheConfig.getCacheClass();

        log.debug("Initializing entry cache "+cacheClass);
        Class clazz = Class.forName(cacheClass);
        entryCache = (EntryCache)clazz.newInstance();

        entryCache.setCacheConfig(cacheConfig);
        entryCache.setConnectionManager(connectionManager);

        entryCache.init();

        engineFilterTool      = new EngineFilterTool(this);
        loadEngine      = new LoadEngine(this);
        mergeEngine     = new MergeEngine(this);
        joinEngine      = new JoinEngine(this);
        transformEngine = new TransformEngine(this);

        log.debug("Proxy engine initialized.");
    }

    public String convertDn(String dn, String oldSuffix, String newSuffix) throws Exception {

        if (dn == null) return null;
        if (!dn.toLowerCase().endsWith(oldSuffix.toLowerCase())) return null;

        dn = dn.substring(0, dn.length() - oldSuffix.length());
        if (dn.endsWith(",")) dn = dn.substring(0, dn.length()-1);

        return EntryUtil.append(dn, newSuffix);
    }

    public int bind(
            PenroseSession session,
            Partition partition,
            String bindDn,
            String password
    ) throws Exception {

        EntryMapping entryMapping = partition.findEntryMapping(bindDn);

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
        String proxyBaseDn = sourceConfig.getParameter(PROXY_BASE_DN);
        bindDn = convertDn(bindDn, proxyDn, proxyBaseDn);

        log.debug("Binding via proxy "+sourceName+" as \""+bindDn +"\" with "+password);

        Map parameters = new HashMap();
        parameters.putAll(connection.getParameters());

        LDAPClient client = new LDAPClient(parameters);

        try {
            return client.bind(bindDn, password);

        } catch (Exception e) {
            return ExceptionUtil.getReturnCode(e);

        } finally {
            if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
                log.debug("Storing connection info in session.");
                if (session != null) session.setAttribute(partition.getName()+"."+sourceName+".connection", client);
            } else {
                client.close();
            }
        }
    }

    public int add(
            PenroseSession session, Partition partition,
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

        String proxyDn = proxyMapping.getDn();
        String proxyBaseDn = sourceConfig.getParameter(PROXY_BASE_DN);
        String targetDn = convertDn(dn, proxyDn, proxyBaseDn);

        log.debug("Modifying via proxy "+sourceName+" as \""+targetDn+"\"");

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);

        LDAPClient client;

        if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
            log.debug("Getting connection info from session.");
            client = session == null ? null : (LDAPClient)session.getAttribute(partition.getName()+"."+sourceName+".connection");

            if (client == null) {

                String rootDn = schemaManager.normalize(penroseConfig.getRootDn());
                String bindDn = schemaManager.normalize(session == null ? null : session.getBindDn());

                if (session == null || rootDn.equals(bindDn)) {
                    log.debug("Creating new connection.");
                    Map parameters = new HashMap();
                    parameters.putAll(connection.getParameters());

                    client = new LDAPClient(parameters);

                } else {
                    log.debug("Missing credentials.");
                    return LDAPException.SUCCESS;
                }
            }

        } else {
            log.debug("Creating new connection.");
            Map parameters = new HashMap();
            parameters.putAll(connection.getParameters());

            client = new LDAPClient(parameters);
        }

        try {
            client.add(targetDn, attributes);

            return LDAPException.SUCCESS;

        } catch (Exception e) {
            return ExceptionUtil.getReturnCode(e);

        } finally {
            client.close();
        }
    }

    public int modify(
            PenroseSession session, Partition partition,
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

        String proxyDn = entryMapping.getDn();
        String proxyBaseDn = sourceConfig.getParameter(PROXY_BASE_DN);
        String targetDn = convertDn(dn, proxyDn, proxyBaseDn);

        log.debug("Modifying via proxy "+sourceName+" as \""+targetDn+"\"");

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);

        LDAPClient client;

        if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
            log.debug("Getting connection info from session.");
            client = session == null ? null : (LDAPClient)session.getAttribute(partition.getName()+"."+sourceName+".connection");

            if (client == null) {

                String rootDn = schemaManager.normalize(penroseConfig.getRootDn());
                String bindDn = schemaManager.normalize(session == null ? null : session.getBindDn());

                if (session == null || rootDn.equals(bindDn)) {
                    log.debug("Creating new connection.");
                    Map parameters = new HashMap();
                    parameters.putAll(connection.getParameters());

                    client = new LDAPClient(parameters);

                } else {
                    log.debug("Missing credentials.");
                    return LDAPException.SUCCESS;
                }
            }

        } else {
            log.debug("Creating new connection.");
            Map parameters = new HashMap();
            parameters.putAll(connection.getParameters());

            client = new LDAPClient(parameters);
        }

        try {
            client.modify(targetDn, modifications);

            return LDAPException.SUCCESS;

        } catch (Exception e) {
            return ExceptionUtil.getReturnCode(e);

        } finally {
            client.close();
        }
    }

    public int modrdn(
            PenroseSession session, Partition partition,
            Entry entry,
            String newRdn, boolean deleteOldRdn) throws Exception {

        EntryMapping entryMapping = entry.getEntryMapping();
        String dn = entry.getDn();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);

        String proxyDn = entryMapping.getDn();
        String proxyBaseDn = sourceConfig.getParameter(PROXY_BASE_DN);
        String targetDn = convertDn(dn, proxyDn, proxyBaseDn);

        log.debug("Renaming via proxy "+sourceName+" as \""+targetDn+"\"");

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);

        LDAPClient client;

        if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
            log.debug("Getting connection info from session.");
            client = session == null ? null : (LDAPClient)session.getAttribute(partition.getName()+"."+sourceName+".connection");

            if (client == null) {

                String rootDn = schemaManager.normalize(penroseConfig.getRootDn());
                String bindDn = schemaManager.normalize(session == null ? null : session.getBindDn());

                if (session == null || rootDn.equals(bindDn)) {
                    log.debug("Creating new connection.");
                    Map parameters = new HashMap();
                    parameters.putAll(connection.getParameters());

                    client = new LDAPClient(parameters);

                } else {
                    log.debug("Missing credentials.");
                    return LDAPException.SUCCESS;
                }
            }

        } else {
            log.debug("Creating new connection.");
            Map parameters = new HashMap();
            parameters.putAll(connection.getParameters());

            client = new LDAPClient(parameters);
        }

        try {
            client.modrdn(targetDn, newRdn);

            return LDAPException.SUCCESS;

        } catch (Exception e) {
            return ExceptionUtil.getReturnCode(e);

        } finally {
            client.close();
        }
    }

    public int delete(
            PenroseSession session, Partition partition,
            Entry entry
    ) throws Exception {

        EntryMapping entryMapping = entry.getEntryMapping();
        String dn = entry.getDn();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        Connection connection = connectionManager.getConnection(partition, connectionName);

        String proxyDn = entryMapping.getDn();
        String proxyBaseDn = sourceConfig.getParameter(PROXY_BASE_DN);
        String targetDn = convertDn(dn, proxyDn, proxyBaseDn);

        log.debug("Deleting via proxy "+sourceName+" as \""+targetDn+"\"");

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);

        LDAPClient client;

        if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
            log.debug("Getting connection info from session.");
            client = session == null ? null : (LDAPClient)session.getAttribute(partition.getName()+"."+sourceName+".connection");

            if (client == null) {

                String rootDn = schemaManager.normalize(penroseConfig.getRootDn());
                String bindDn = schemaManager.normalize(session == null ? null : session.getBindDn());

                if (session == null || rootDn.equals(bindDn)) {
                    log.debug("Creating new connection.");
                    Map parameters = new HashMap();
                    parameters.putAll(connection.getParameters());

                    client = new LDAPClient(parameters);

                } else {
                    log.debug("Missing credentials.");
                    return LDAPException.SUCCESS;
                }
            }

        } else {
            log.debug("Creating new connection.");
            Map parameters = new HashMap();
            parameters.putAll(connection.getParameters());

            client = new LDAPClient(parameters);
        }

        try {
            client.delete(targetDn);

            return LDAPException.SUCCESS;

        } catch (Exception e) {
            return ExceptionUtil.getReturnCode(e);

        } finally {
            client.close();
        }
    }

    public int search(
            PenroseSession session,
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

            expand(session, partition, parentPath, parentSourceValues, entryMapping, baseDn, filter, sc, results);
        }

        results.close();

        return LDAPException.SUCCESS;
    }

    public int expand(
            PenroseSession session,
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

        final String proxyDn = entryMapping.getDn();
        log.debug("Proxy DN: "+proxyDn);

        final String proxyBaseDn = sourceConfig.getParameter(PROXY_BASE_DN);
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
            targetDn = convertDn(baseDn, proxyDn, proxyBaseDn);

        } else {
            if (sc.getScope() == LDAPConnection.SCOPE_BASE) {
                return LDAPException.SUCCESS;

            } else if (sc.getScope() == LDAPConnection.SCOPE_ONE) {
                newSc.setScope(LDAPConnection.SCOPE_BASE);
            }
            targetDn = EntryUtil.append(targetDn, proxyBaseDn);
        }

        log.debug("Searching proxy "+sourceName+" for \""+targetDn+"\" with filter="+filter+" attrs="+newSc.getAttributes());

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);

        LDAPClient client;

        if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
            log.debug("Getting connection info from session.");
            client = session == null ? null : (LDAPClient)session.getAttribute(partition.getName()+"."+sourceName+".connection");

            if (client == null) {

                String rootDn = schemaManager.normalize(penroseConfig.getRootDn());
                String bindDn = schemaManager.normalize(session == null ? null : session.getBindDn());

                if (session == null || rootDn.equals(bindDn)) {
                    log.debug("Creating new connection.");
                    Map parameters = new HashMap();
                    parameters.putAll(connection.getParameters());

                    client = new LDAPClient(parameters);

                } else {
                    log.debug("Missing credentials.");
                    results.close();
                    return LDAPException.SUCCESS;
                }
            }

        } else {
            log.debug("Creating new connection.");
            Map parameters = new HashMap();
            parameters.putAll(connection.getParameters());

            client = new LDAPClient(parameters);
        }

        PenroseSearchResults res = new PenroseSearchResults();

        res.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    SearchResult ldapEntry = (SearchResult)event.getObject();
                    String dn = ldapEntry.getName();
                    //String dn = EntryUtil.append(ldapEntry.getName(), proxyBaseDn);

                    log.debug("Renaming \""+dn+"\"");

                    dn = convertDn(dn, proxyBaseDn, proxyDn);
/*
                    if (proxyBaseDn != null) {
                        dn = dn.substring(0, ldapEntry.getName().length() - proxyBaseDn.length());
                        if (dn.endsWith(",")) dn = dn.substring(0, dn.length()-1);
                    }

                    //dn = "".equals(ldapEntry.getDN()) ? base : ldapEntry.getDN()+","+base;
*/
                    //dn = EntryUtil.append(dn, proxyDn);
                    log.debug("into "+dn);

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

        try {
            client.search(targetDn, filter.toString(), newSc, res);

            return LDAPException.SUCCESS;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            int rc = ExceptionUtil.getReturnCode(e);
            results.setReturnCode(rc);
            return rc;

        } finally {
            client.close();
        }
    }

    public Entry find(
            PenroseSession session,
            Partition partition,
            Collection parentPath,
            AttributeValues parentSourceValues,
            EntryMapping entryMapping,
            String dn
    ) throws Exception {

        PenroseSearchResults results = new PenroseSearchResults();

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_BASE);

        Row rdn = EntryUtil.getRdn(dn);
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

        if (!results.hasNext()) return null;

        return (Entry)results.next();
    }
}
