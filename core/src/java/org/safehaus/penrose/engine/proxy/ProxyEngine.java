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
import org.safehaus.penrose.pipeline.Pipeline;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineFilterTool;
import org.safehaus.penrose.engine.TransformEngine;
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

    public LDAPClient createClient(PenroseSession session, Partition partition, SourceConfig sourceConfig) throws Exception {

        boolean debug = log.isDebugEnabled();

        String authentication = sourceConfig.getParameter(PROXY_AUTHENTICATON);
        if (debug) log.debug("Authentication: "+authentication);

        if (PROXY_AUTHENTICATON_DISABLED.equals(authentication)) {
            if (debug) log.debug("Pass-Through Authentication is disabled.");
            throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
        }

        String connectionName = sourceConfig.getConnectionName();
        Connection connection = connectionManager.getConnection(partition, connectionName);

        Map parameters = new HashMap();
        parameters.putAll(connection.getParameters());

        return new LDAPClient(parameters);
    }

    public void storeClient(PenroseSession session, Partition partition, SourceConfig sourceConfig, LDAPClient client) throws Exception {

        boolean debug = log.isDebugEnabled();

        String authentication = sourceConfig.getParameter(PROXY_AUTHENTICATON);
        if (debug) log.debug("Authentication: "+authentication);

        if (PROXY_AUTHENTICATON_FULL.equals(authentication)) {
            if (debug) log.debug("Storing connection info in session.");

            String connectionName = sourceConfig.getConnectionName();
            Connection connection = connectionManager.getConnection(partition, connectionName);

            if (session != null) session.setAttribute(partition.getName()+".connection."+connection.getName(), client);
        } else {
            try { if (client != null) client.close(); } catch (Exception e) {}
        }
    }

    public void closeClient(PenroseSession session, Partition partition, SourceConfig sourceConfig, LDAPClient client) throws Exception {

        boolean debug = log.isDebugEnabled();

        String authentication = sourceConfig.getParameter(PROXY_AUTHENTICATON);
        if (debug) log.debug("Authentication: "+authentication);

        if (!PROXY_AUTHENTICATON_FULL.equals(authentication)) {
            try { if (client != null) client.close(); } catch (Exception e) {}
        }
    }

    public LDAPClient getClient(PenroseSession session, Partition partition, SourceConfig sourceConfig) throws Exception {

        boolean debug = log.isDebugEnabled();

        String authentication = sourceConfig.getParameter(PROXY_AUTHENTICATON);
        if (debug) log.debug("Authentication: "+authentication);

        Connection connection = connectionManager.getConnection(partition, sourceConfig.getConnectionName());
        LDAPClient client = null;

        if (PROXY_AUTHENTICATON_FULL.equals(authentication)) {
            if (debug) log.debug("Getting connection info from session.");

            client = session == null ? null : (LDAPClient)session.getAttribute(partition.getName()+".connection."+connection.getName());

            if (client == null) {

                if (session == null || session.isRootUser()) {
                    if (debug) log.debug("Creating new connection.");

                    Map parameters = new HashMap();
                    parameters.putAll(connection.getParameters());

                    client = new LDAPClient(parameters);

                } else {
                    if (debug) log.debug("Missing credentials.");
                    throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
                }
            }

        } else {
            if (debug) log.debug("Creating new connection.");
            Map parameters = new HashMap();
            parameters.putAll(connection.getParameters());

            client = new LDAPClient(parameters);
        }

        return client;
    }

    public void bind(
            PenroseSession session,
            Partition partition,
            EntryMapping entryMapping,
            DN dn,
            String password
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        LDAPClient client = createClient(session, partition, sourceConfig);

        try {
            DN proxyDn = entryMapping.getDn();
            DN proxyBaseDn = new DN(sourceConfig.getParameter(PROXY_BASE_DN));
            DN bindDn = convertDn(dn, proxyDn, proxyBaseDn);

            if (debug) log.debug("Binding via proxy as \""+bindDn +"\" with "+password);

            client.bind(bindDn.toString(), password);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            storeClient(session, partition, sourceConfig, client);
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

        boolean debug = log.isDebugEnabled();

        EntryMapping proxyMapping = parent.getEntryMapping();

        SourceMapping sourceMapping = proxyMapping.getSourceMapping(0);
        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

        LDAPClient client = getClient(session, partition, sourceConfig);

        try {
            DN proxyDn = proxyMapping.getDn();
            DN proxyBaseDn = new DN(sourceConfig.getParameter(PROXY_BASE_DN));
            DN targetDn = convertDn(dn, proxyDn, proxyBaseDn);

            if (debug) log.debug("Modifying via proxy as \""+targetDn+"\"");

            client.add(targetDn.toString(), attributes);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            closeClient(session, partition, sourceConfig, client);
        }
    }

    public void modify(
            PenroseSession session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            DN dn,
            Collection modifications
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        LDAPClient client = getClient(session, partition, sourceConfig);

        try {
            DN proxyDn = entryMapping.getDn();
            DN proxyBaseDn = new DN(sourceConfig.getParameter(PROXY_BASE_DN));
            DN targetDn = convertDn(dn, proxyDn, proxyBaseDn);

            if (debug) log.debug("Modifying via proxy as \""+targetDn+"\"");

            client.modify(targetDn.toString(), modifications);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            closeClient(session, partition, sourceConfig, client);
        }
    }

    public void modrdn(
            PenroseSession session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            DN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        LDAPClient client = getClient(session, partition, sourceConfig);

        try {
            DN proxyDn = entryMapping.getDn();
            DN proxyBaseDn = new DN(sourceConfig.getParameter(PROXY_BASE_DN));
            DN targetDn = convertDn(dn, proxyDn, proxyBaseDn);

            if (debug) log.debug("Renaming via proxy as \""+targetDn+"\"");

            client.modrdn(targetDn.toString(), newRdn.toString());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            closeClient(session, partition, sourceConfig, client);
        }
    }

    public void delete(
            PenroseSession session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            DN dn
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        LDAPClient client = getClient(session, partition, sourceConfig);

        try {
            DN proxyDn = entryMapping.getDn();
            DN proxyBaseDn = new DN(sourceConfig.getParameter(PROXY_BASE_DN));
            DN targetDn = convertDn(dn, proxyDn, proxyBaseDn);

            if (debug) log.debug("Deleting via proxy as \""+targetDn+"\"");

            client.delete(targetDn.toString());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            closeClient(session, partition, sourceConfig, client);
        }
    }

    public Entry find(
            Partition partition,
            AttributeValues sourceValues,
            EntryMapping entryMapping,
            DN dn
    ) throws Exception {

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

        PenroseSearchResults results = new PenroseSearchResults();

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_BASE);

        RDN rdn = dn.getRdn();
        Filter filter = FilterTool.createFilter(rdn);

        search(
                null,
                partition,
                sourceValues,
                entryMapping,
                dn,
                filter,
                sc,
                results
        );

        Entry entry = null;
        if (results.hasNext() && getFilterTool().isValid(entry, filter)) {
            entry = (Entry)results.next();
        }

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("FIND RESULT", 80));

            if (entry == null) {
                log.debug(Formatter.displayLine("Entry \""+dn+"\" not found", 80));
            } else {
                log.debug(Formatter.displayLine(" - "+(entry == null ? null : entry.getDn()), 80));
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


        return entry;
    }

    public void search(
            final PenroseSession session,
            final Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final DN baseDn,
            final Filter filter,
            final PenroseSearchControls searchControls,
            final Results results
    ) throws Exception {

        final boolean debug = log.isDebugEnabled();

        long sizeLimit = searchControls.getSizeLimit();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH", 80));
            log.debug(Formatter.displayLine("Mapping DN: \""+entryMapping.getDn()+"\"", 80));
            log.debug(Formatter.displayLine("Base DN: "+baseDn, 80));
            log.debug(Formatter.displayLine("Filter: "+filter, 80));
            log.debug(Formatter.displayLine("Scope: "+LDAPUtil.getScope(searchControls.getScope()), 80));
            log.debug(Formatter.displayLine("Size Limit: "+sizeLimit, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);
        final SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        try {
            final DN proxyDn = entryMapping.getDn();
            if (debug) log.debug("Proxy DN: "+proxyDn);

            final DN proxyBaseDn = new DN(sourceConfig.getParameter(PROXY_BASE_DN));
            if (debug) log.debug("Proxy Base DN: "+proxyBaseDn);

            if (debug) log.debug("Checking whether "+entryMapping.getDn()+" is an ancestor of "+baseDn);
            boolean found = baseDn.endsWith(entryMapping.getDn());

            if (debug) log.debug("Result: "+found);

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

            if (debug) log.debug("Searching proxy for \""+targetDn+"\" with filter="+filter+" attrs="+sc.getAttributes());

            final LDAPClient client = getClient(session, partition, sourceConfig);

            if (debug) log.debug("Creating SearchResult to Entry conversion pipeline.");

            Pipeline sr = new Pipeline(results) {
                public void add(Object object) throws Exception {
                    SearchResult ldapEntry = (SearchResult)object;

                    DN dn = new DN(ldapEntry.getName());
                    //String dn = EntryUtil.append(ldapEntry.getName(), proxyBaseDn);

                    if (debug) log.debug("Renaming \""+dn+"\"");

                    dn = convertDn(dn, proxyBaseDn, proxyDn);
                    if (debug) log.debug("into "+dn);

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

                    Entry entry = new Entry(dn.toString(), entryMapping, attributeValues);
                    super.add(entry);
                }

                public void close() throws Exception {
                    if (debug) log.debug("Closing SearchResult to Entry conversion pipeline.");
                    closeClient(session, partition, sourceConfig, client);
                }
            };

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
