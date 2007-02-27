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
package org.safehaus.penrose.engine.proxy;

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
import org.safehaus.penrose.engine.*;
import org.safehaus.penrose.entry.*;
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
        transformEngine  = new TransformEngine(this);

        log.debug("Proxy engine initialized.");
    }

    public DN convertDn(DN dn, DN oldSuffix, DN newSuffix) throws Exception {

        if (dn == null || dn.isEmpty()) return null;

        if (!dn.endsWith(oldSuffix)) {
            log.debug("["+dn+"] is not a decendant of ["+oldSuffix+"]");
            return null;
        }

        int start = dn.getSize() - oldSuffix.getSize();

        DNBuilder newDn = new DNBuilder();
        for (int i=0; i<start; i++) {
            RDN rdn = dn.get(i);
            newDn.append(rdn);
        }

        newDn.append(newSuffix);

        return newDn.toDn();
    }

    public void bind(
            PenroseSession session,
            Partition partition,
            EntryMapping entryMapping,
            DN dn,
            String password
    ) throws Exception {

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);
        if (PROXY_AUTHENTICATON_DISABLED.equals(pta)) {
            log.debug("Pass-Through Authentication is disabled.");
            throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
        }

        LDAPClient client = null;

        try {
            Connection connection = connectionManager.getConnection(partition, connectionName);

            DN proxyDn = entryMapping.getDn();
            DN proxyBaseDn = new DN(sourceConfig.getParameter(PROXY_BASE_DN));
            DN bindDn = convertDn(dn, proxyDn, proxyBaseDn);

            log.debug("Binding via proxy "+sourceName+" as \""+bindDn +"\" with "+password);

            Map parameters = new HashMap();
            parameters.putAll(connection.getParameters());

            client = new LDAPClient(parameters);
            client.bind(bindDn.toString(), password);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

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
            PenroseSession session,
            Partition partition,
            Entry parent,
            EntryMapping entryMapping,
            DN dn,
            Attributes attributes
    ) throws Exception {

        EntryMapping proxyMapping = parent.getEntryMapping();

        SourceMapping sourceMapping = proxyMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);

        LDAPClient client = null;

        try {
            Connection connection = connectionManager.getConnection(partition, connectionName);

            DN proxyDn = proxyMapping.getDn();
            DN proxyBaseDn = new DN(sourceConfig.getParameter(PROXY_BASE_DN));
            DN targetDn = convertDn(dn, proxyDn, proxyBaseDn);

            log.debug("Modifying via proxy "+sourceName+" as \""+targetDn+"\"");

            if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
                log.debug("Getting connection info from session.");
                client = session == null ? null : (LDAPClient)session.getAttribute(partition.getName()+"."+sourceName+".connection");

                if (client == null) {

                    if (session == null || session.isRootUser()) {
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

            client.add(targetDn.toString(), attributes);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            if (!PROXY_AUTHENTICATON_FULL.equals(pta)) {
                try { if (client != null) client.close(); } catch (Exception e) {}
            }
        }
    }

    public void modify(
            PenroseSession session,
            Partition partition,
            Entry entry,
            Collection modifications
    ) throws Exception {

        EntryMapping entryMapping = entry.getEntryMapping();
        DN dn = entry.getDn();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);

        LDAPClient client = null;

        try {
            Connection connection = connectionManager.getConnection(partition, connectionName);

            DN proxyDn = entryMapping.getDn();
            DN proxyBaseDn = new DN(sourceConfig.getParameter(PROXY_BASE_DN));
            DN targetDn = convertDn(dn, proxyDn, proxyBaseDn);

            log.debug("Modifying via proxy "+sourceName+" as \""+targetDn+"\"");

            if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
                log.debug("Getting connection info from session.");
                client = session == null ? null : (LDAPClient)session.getAttribute(partition.getName()+"."+sourceName+".connection");

                if (client == null) {

                    if (session == null || session.isRootUser()) {
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

            client.modify(targetDn.toString(), modifications);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            if (!PROXY_AUTHENTICATON_FULL.equals(pta)) {
                try { if (client != null) client.close(); } catch (Exception e) {}
            }
        }
    }

    public void modrdn(
            PenroseSession session,
            Partition partition,
            Entry entry,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws LDAPException {

        EntryMapping entryMapping = entry.getEntryMapping();
        DN dn = entry.getDn();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);

        LDAPClient client = null;

        try {
            Connection connection = connectionManager.getConnection(partition, connectionName);

            DN proxyDn = entryMapping.getDn();
            DN proxyBaseDn = new DN(sourceConfig.getParameter(PROXY_BASE_DN));
            DN targetDn = convertDn(dn, proxyDn, proxyBaseDn);

            log.debug("Renaming via proxy "+sourceName+" as \""+targetDn+"\"");

            if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
                log.debug("Getting connection info from session.");
                client = session == null ? null : (LDAPClient)session.getAttribute(partition.getName()+"."+sourceName+".connection");

                if (client == null) {

                    if (session == null || session.isRootUser()) {
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

            client.modrdn(targetDn.toString(), newRdn.toString());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            if (!PROXY_AUTHENTICATON_FULL.equals(pta)) {
                try { if (client != null) client.close(); } catch (Exception e) {}
            }
        }
    }

    public void delete(
            PenroseSession session,
            Partition partition,
            Entry entry
    ) throws LDAPException {

        EntryMapping entryMapping = entry.getEntryMapping();
        DN dn = entry.getDn();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        String sourceName = sourceMapping.getSourceName();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
        String connectionName = sourceConfig.getConnectionName();

        String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);

        LDAPClient client = null;

        try {
            Connection connection = connectionManager.getConnection(partition, connectionName);

            DN proxyDn = entryMapping.getDn();
            DN proxyBaseDn = new DN(sourceConfig.getParameter(PROXY_BASE_DN));
            DN targetDn = convertDn(dn, proxyDn, proxyBaseDn);

            log.debug("Deleting via proxy "+sourceName+" as \""+targetDn+"\"");

            if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
                log.debug("Getting connection info from session.");
                client = session == null ? null : (LDAPClient)session.getAttribute(partition.getName()+"."+sourceName+".connection");

                if (client == null) {

                    if (session == null || session.isRootUser()) {
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

            client.delete(targetDn.toString());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            if (!PROXY_AUTHENTICATON_FULL.equals(pta)) {
                try { if (client != null) client.close(); } catch (Exception e) {}
            }
        }
    }

    public List find(
            Partition partition,
            AttributeValues sourceValues,
            EntryMapping entryMapping,
            List rdns,
            int position
    ) throws Exception {

        DNBuilder db = new DNBuilder();
        for (int i = 0; i < rdns.size(); i++) {
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

        PenroseSearchResults results = new PenroseSearchResults();

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_BASE);

        RDN rdn = dn.getRdn();
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

    public void search(
            final PenroseSession session,
            final Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping entryMapping,
            final DN baseDn,
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
    }

    public void expand(
            final PenroseSession session,
            final Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping entryMapping,
            final DN baseDn,
            final Filter filter,
            final PenroseSearchControls searchControls,
            final PenroseSearchResults results
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

        final String pta = sourceConfig.getParameter(PROXY_AUTHENTICATON);

        try {
            String connectionName = sourceConfig.getConnectionName();
            Connection connection = connectionManager.getConnection(partition, connectionName);

            final DN proxyDn = entryMapping.getDn();
            log.debug("Proxy DN: "+proxyDn);

            final DN proxyBaseDn = new DN(sourceConfig.getParameter(PROXY_BASE_DN));
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
            DN dn = baseDn;
            boolean found = false;
            while (dn != null) {
                if (dn.matches(entryMapping.getDn())) {
                    found = true;
                    break;
                }
                dn = dn.getParentDn();
            }

            log.debug("Result: "+found);

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setAttributes(searchControls.getAttributes());
            sc.setScope(searchControls.getScope());
            sc.setSizeLimit(searchControls.getSizeLimit());
            sc.setTimeLimit(searchControls.getTimeLimit());

            DN targetDn = null;
            if (found) {
                targetDn = convertDn(baseDn, proxyDn, proxyBaseDn);

            } else {
                if (searchControls.getScope() == LDAPConnection.SCOPE_BASE) {
                    return;

                } else if (searchControls.getScope() == LDAPConnection.SCOPE_ONE) {
                    sc.setScope(LDAPConnection.SCOPE_BASE);
                }
                targetDn = proxyBaseDn;
            }

            log.debug("Searching proxy "+sourceName+" for \""+targetDn+"\" with filter="+filter+" attrs="+sc.getAttributes());

            LDAPClient client;

            if (PROXY_AUTHENTICATON_FULL.equals(pta)) {
                log.debug("Getting connection info from session.");
                client = session == null ? null : (LDAPClient)session.getAttribute(partition.getName()+"."+sourceName+".connection");

                if (client == null) {

                    if (session == null || session.isRootUser()) {
                        log.debug("Creating new connection.");
                        Map parameters = new HashMap();
                        parameters.putAll(connection.getParameters());

                        client = new LDAPClient(parameters);

                    } else {
                        log.debug("Missing credentials.");
                        throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
                    }
                }

            } else {
                log.debug("Creating new connection.");
                Map parameters = new HashMap();
                parameters.putAll(connection.getParameters());

                client = new LDAPClient(parameters);
            }

            final LDAPClient finalClient = client;

            PenroseSearchResults sr = new PenroseSearchResults();

            sr.addListener(new PipelineAdapter() {
                public void objectAdded(PipelineEvent event) {
                    try {
                        SearchResult ldapEntry = (SearchResult)event.getObject();
                        DN dn = new DN(ldapEntry.getName());
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
                        results.add(entry);

                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }

                public void pipelineClosed(PipelineEvent event) throws Exception {
                    log.debug("Closing SearchResult to Entry conversion pipeline.");
                    if (!PROXY_AUTHENTICATON_FULL.equals(pta)) {
                        finalClient.close();
                    }
                }
            });

            sr.addReferralListener(new ReferralAdapter() {
                public void referralAdded(ReferralEvent event) {
                    Object referral = event.getReferral();
                    //log.debug("Passing referral: "+referral);
                    results.addReferral(referral);
                }
            });


            //connector.search(partition, sourceConfig, null, filter, sc, sr);
            client.search(targetDn.toString(), filter.toString(), sc, sr);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            results.close();
        }
    }
}
