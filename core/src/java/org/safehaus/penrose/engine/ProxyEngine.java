package org.safehaus.penrose.engine;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.connector.Connection;
import org.safehaus.penrose.ldap.LDAPClient;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.util.Formatter;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPConnection;

import javax.naming.directory.*;
import javax.naming.NamingEnumeration;
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

    public void init() throws Exception {
        super.init();

        engineFilterTool = new EngineFilterTool(this);
        loadEngine       = new LoadEngine(this);
        mergeEngine      = new MergeEngine(this);
        joinEngine       = new JoinEngine(this);
        transformEngine  = new TransformEngine(this);

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
            EntryMapping entryMapping, String bindDn,
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
            AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            Entry baseEntry,
            final String baseDn,
            final Filter filter,
            PenroseSearchControls sc,
            final PenroseSearchResults results
    ) throws Exception {

        if (sc.getScope() == LDAPConnection.SCOPE_BASE) {

            results.add(baseEntry);

        } else {

            expand(
                    session,
                    partition,
                    baseEntry,
                    parentSourceValues,
                    entryMapping,
                    baseDn,
                    filter,
                    sc,
                    results
            );
        }

        results.close();

        return LDAPException.SUCCESS;
    }

    public int expand(
            PenroseSession session,
            final Partition partition,
            Entry baseEntry,
            AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            final String baseDn,
            final Filter filter,
            PenroseSearchControls searchControls,
            final PenroseSearchResults searchResults
    ) throws Exception {

        long sizeLimit = searchControls.getSizeLimit();

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("EXPAND PROXY", 80));
            log.debug(Formatter.displayLine("Entry: \""+entryMapping.getDn()+"\"", 80));
            log.debug(Formatter.displayLine("Base DN: "+baseDn, 80));
            log.debug(Formatter.displayLine("Filter: "+filter, 80));
            log.debug(Formatter.displayLine("Scope: "+LDAPUtil.getScope(searchControls.getScope()), 80));
            log.debug(Formatter.displayLine("Size Limit: "+sizeLimit, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        //Connector connector = getConnector(sourceConfig);

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

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setAttributes(searchControls.getAttributes());
        sc.setScope(searchControls.getScope());
        sc.setSizeLimit(searchControls.getSizeLimit());
        sc.setTimeLimit(searchControls.getTimeLimit());

        String targetDn = "";
        if (found) {
            targetDn = convertDn(baseDn, proxyDn, proxyBaseDn);

        } else {
            if (searchControls.getScope() == LDAPConnection.SCOPE_BASE) {
                return LDAPException.SUCCESS;

            } else if (searchControls.getScope() == LDAPConnection.SCOPE_ONE) {
                sc.setScope(LDAPConnection.SCOPE_BASE);
            }
            targetDn = EntryUtil.append(targetDn, proxyBaseDn);
        }

        log.debug("Searching proxy "+sourceName+" for \""+targetDn+"\" with filter="+filter+" attrs="+sc.getAttributes());

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
                    searchResults.close();
                    return LDAPException.SUCCESS;
                }
            }

        } else {
            log.debug("Creating new connection.");
            Map parameters = new HashMap();
            parameters.putAll(connection.getParameters());

            client = new LDAPClient(parameters);
        }

        PenroseSearchResults sr = new PenroseSearchResults();

        sr.addListener(new PipelineAdapter() {
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
                    searchResults.add(entry);

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        });

        sr.addReferralListener(new ReferralAdapter() {
            public void referralAdded(ReferralEvent event) {
                Object referral = event.getReferral();
                //log.debug("Passing referral: "+referral);
                searchResults.addReferral(referral);
            }
        });

        try {

            //connector.search(partition, sourceConfig, null, filter, sc, sr);
            client.search(targetDn, filter.toString(), sc, sr);

            return LDAPException.SUCCESS;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            int rc = ExceptionUtil.getReturnCode(e);
            searchResults.setReturnCode(rc);
            return rc;

        } finally {
            client.close();
        }
    }

    public Entry find(
            Partition partition,
            Entry parent,
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
                null,
                partition,
                parent,
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
