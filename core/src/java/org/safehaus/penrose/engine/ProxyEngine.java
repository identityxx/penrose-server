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
import org.safehaus.penrose.interpreter.Interpreter;
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

        List rdns1 = EntryUtil.parseDn(dn);
        List rdns2 = EntryUtil.parseDn(oldSuffix);

        if (rdns1.size() < rdns2.size()) {
            log.debug("["+dn+"] is not a decendant of ["+oldSuffix+"]");
            return null;
        }

        int start = rdns1.size() - rdns2.size();

        for (int i=0; i<rdns2.size(); i++) {
            Row rdn1 = (Row)rdns1.get(start+i);
            Row rdn2 = (Row)rdns2.get(i);

            rdn1 = getSchemaManager().normalize(rdn1);
            rdn2 = getSchemaManager().normalize(rdn2);

            if (rdn1.equals(rdn2)) continue;

            log.debug("["+rdn1+"] does not match ["+rdn2+"]");
            return null;
        }

        String newDn = null;
        for (int i=0; i<start; i++) {
            Row rdn = (Row)rdns1.get(i);
            newDn = EntryUtil.append(newDn, rdn);
        }

        return EntryUtil.append(newDn, newSuffix);
    }

    public void bind(
            PenroseSession session,
            Partition partition,
            EntryMapping entryMapping, String bindDn,
            String password
    ) throws LDAPException {

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);
        if (PROXY_AUTHENTICATON_DISABLED.equals(pta)) {
            log.debug("Pass-Through Authentication is disabled.");
            int rc = LDAPException.INVALID_CREDENTIALS;
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
        }

        LDAPClient client = null;

        try {
            Connection connection = connectionManager.getConnection(partition, connectionName);

            String proxyDn = entryMapping.getDn();
            String proxyBaseDn = sourceConfig.getParameter(PROXY_BASE_DN);
            bindDn = convertDn(bindDn, proxyDn, proxyBaseDn);

            log.debug("Binding via proxy "+sourceName+" as \""+bindDn +"\" with "+password);

            Map parameters = new HashMap();
            parameters.putAll(connection.getParameters());

            client = new LDAPClient(parameters);
            client.bind(bindDn, password);

        } catch (Exception e) {
            int rc = LDAPException.INVALID_CREDENTIALS;
            String message = e.getMessage();
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
                log.debug("Storing connection info in session.");
                if (session != null) session.setAttribute(partition.getName()+"."+sourceName+".connection", client);
            } else {
                try { if (client != null) client.close(); } catch (Exception e) {}
            }
        }
    }

    public void add(
            PenroseSession session, Partition partition,
            Entry parent,
            EntryMapping entryMapping,
            String dn,
            Attributes attributes
    ) throws LDAPException {

        LDAPClient client = null;

        try {
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
                        return;
                    }
                }

            } else {
                log.debug("Creating new connection.");
                Map parameters = new HashMap();
                parameters.putAll(connection.getParameters());

                client = new LDAPClient(parameters);
            }

            client.add(targetDn, attributes);

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            try { if (client != null) client.close(); } catch (Exception e) {}
        }
    }

    public void modify(
            PenroseSession session, Partition partition,
            Entry entry,
            Collection modifications
    ) throws LDAPException {

        LDAPClient client = null;

        try {
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
                        return;
                    }
                }

            } else {
                log.debug("Creating new connection.");
                Map parameters = new HashMap();
                parameters.putAll(connection.getParameters());

                client = new LDAPClient(parameters);
            }

            client.modify(targetDn, modifications);

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            try { if (client != null) client.close(); } catch (Exception e) {}
        }
    }

    public void modrdn(
            PenroseSession session,
            Partition partition,
            Entry entry,
            String newRdn,
            boolean deleteOldRdn
    ) throws LDAPException {

        LDAPClient client = null;

        try {
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
                        return;
                    }
                }

            } else {
                log.debug("Creating new connection.");
                Map parameters = new HashMap();
                parameters.putAll(connection.getParameters());

                client = new LDAPClient(parameters);
            }

            client.modrdn(targetDn, newRdn);

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            try { if (client != null) client.close(); } catch (Exception e) {}
        }
    }

    public void delete(
            PenroseSession session,
            Partition partition,
            Entry entry
    ) throws LDAPException {

        LDAPClient client = null;

        try {
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
                        return;
                    }
                }

            } else {
                log.debug("Creating new connection.");
                Map parameters = new HashMap();
                parameters.putAll(connection.getParameters());

                client = new LDAPClient(parameters);
            }

            client.delete(targetDn);

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            try { if (client != null) client.close(); } catch (Exception e) {}
        }
    }

    public List find(
            Partition partition,
            AttributeValues sourceValues,
            EntryMapping entryMapping,
            List rdns,
            int position
    ) throws Exception {

        String dn = null;
        for (int i = 0; i < rdns.size(); i++) {
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

        PenroseSearchResults results = new PenroseSearchResults();

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_BASE);

        Row rdn = EntryUtil.getRdn(dn);
        Filter filter = FilterTool.createFilter(rdn);

        expand(
                null,
                partition,
                sourceValues,
                entryMapping,
                dn,
                filter,
                sc,
                results
        );

        results.close();

        if (results.hasNext()) {
            Entry entry = (Entry)results.next();

            path.add(entry);

            for (int i=0; i<rdns.size()-position-1; i++) {
                path.add(null);
            }
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
            final PenroseSession session,
            final Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping entryMapping,
            final String baseDn,
            final Filter filter,
            final PenroseSearchControls sc,
            final PenroseSearchResults results
    ) throws Exception {

        expand(
                session,
                partition,
                sourceValues,
                entryMapping,
                baseDn,
                filter,
                sc,
                results
        );

        return LDAPException.SUCCESS;
    }

    public int expand(
            PenroseSession session,
            final Partition partition,
            AttributeValues sourceValues,
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
            log.debug(Formatter.displayLine("Mapping DN: \""+entryMapping.getDn()+"\"", 80));
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

                    Interpreter interpreter = getInterpreterManager().newInstance();
                    AttributeValues av = computeAttributeValues(entryMapping, interpreter);
                    attributeValues.set(av);

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
}
