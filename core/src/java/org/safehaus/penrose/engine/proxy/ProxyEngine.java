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
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineFilterTool;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPConnection;

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

    public LDAPClient createClient(Session session, Partition partition, SourceConfig sourceConfig) throws Exception {

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

    public void storeClient(Session session, Partition partition, SourceConfig sourceConfig, LDAPClient client) throws Exception {

        boolean debug = log.isDebugEnabled();

        String authentication = sourceConfig.getParameter(PROXY_AUTHENTICATON);
        if (debug) log.debug("Authentication: "+authentication);

        if (PROXY_AUTHENTICATON_FULL.equals(authentication)) {
            if (debug) log.debug("Storing connection info in session.");

            String connectionName = sourceConfig.getConnectionName();
            Connection connection = connectionManager.getConnection(partition, connectionName);

            if (session != null) session.setAttribute(partition.getName()+".connection."+connection.getName(), client);
        } else {
            try { if (client != null) client.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    public void closeClient(Session session, Partition partition, SourceConfig sourceConfig, LDAPClient client) throws Exception {

        boolean debug = log.isDebugEnabled();

        String authentication = sourceConfig.getParameter(PROXY_AUTHENTICATON);
        if (debug) log.debug("Authentication: "+authentication);

        if (!PROXY_AUTHENTICATON_FULL.equals(authentication)) {
            try { if (client != null) client.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    public LDAPClient getClient(Session session, Partition partition, SourceConfig sourceConfig) throws Exception {

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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            Partition partition,
            Entry parent,
            EntryMapping entryMapping,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = request.getDn();
        Attributes attributes = request.getAttributes();

        EntryMapping proxyMapping = parent.getEntryMapping();

        AttributeValues attributeValues = EntryUtil.computeAttributeValues(attributes);
        SourceMapping sourceMapping = proxyMapping.getSourceMapping(0);
        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

        LDAPClient client = getClient(session, partition, sourceConfig);

        try {
            DN proxyDn = proxyMapping.getDn();
            DN proxyBaseDn = new DN(sourceConfig.getParameter(PROXY_BASE_DN));
            DN targetDn = convertDn(dn, proxyDn, proxyBaseDn);

            if (debug) log.debug("Modifying via proxy as \""+targetDn+"\"");

            javax.naming.directory.Attributes attrs = new javax.naming.directory.BasicAttributes();

            for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                javax.naming.directory.Attribute attribute = new javax.naming.directory.BasicAttribute(name);

                Collection values = attributeValues.get(name);
                for (Iterator j=values.iterator(); j.hasNext(); ) {
                    Object value = j.next();
                    attribute.add(value);
                }
            }
            
            client.add(targetDn.toString(), attrs);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            closeClient(session, partition, sourceConfig, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = request.getDn();
        String password = request.getPassword();

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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = request.getDn();

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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = request.getDn();
        Collection modifications = request.getModifications();

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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = request.getDn();
        RDN newRdn = request.getNewRdn();
        boolean deleteOldRdn = request.getDeleteOldRdn();

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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            final Session session,
            final Partition partition,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final AttributeValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        final boolean debug = log.isDebugEnabled();
        final DN baseDn = request.getDn();
        final Filter filter = request.getFilter();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH", 80));
            log.debug(Formatter.displayLine("Mapping DN: \""+entryMapping.getDn()+"\"", 80));
            log.debug(Formatter.displayLine("Base DN: "+baseDn, 80));
            log.debug(Formatter.displayLine("Filter: "+filter, 80));
            log.debug(Formatter.displayLine("Scope: "+LDAPUtil.getScope(request.getScope()), 80));
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

            SearchRequest newRequest = new SearchRequest();
            newRequest.setAttributes(newRequest.getAttributes());
            newRequest.setScope(newRequest.getScope());
            newRequest.setSizeLimit(newRequest.getSizeLimit());
            newRequest.setTimeLimit(newRequest.getTimeLimit());

            DN targetDn = null;
            if (found) {
                targetDn = convertDn(baseDn, proxyDn, proxyBaseDn);

            } else {
                if (newRequest.getScope() == LDAPConnection.SCOPE_BASE) {
                    return;

                } else if (newRequest.getScope() == LDAPConnection.SCOPE_ONE) {
                    newRequest.setScope(LDAPConnection.SCOPE_BASE);
                }
                targetDn = proxyBaseDn;
            }

            if (debug) log.debug("Searching proxy for \""+targetDn+"\" with filter="+filter+" attrs="+ newRequest.getAttributes());

            final LDAPClient client = getClient(session, partition, sourceConfig);

            if (debug) log.debug("Creating SearchResult to Entry conversion pipeline.");

            SearchResponse sr = new SearchResponse() {
                public void add(Object object) throws Exception {
                    javax.naming.directory.SearchResult ldapEntry = (javax.naming.directory.SearchResult)object;

                    DN dn = new DN(ldapEntry.getName());
                    //String dn = EntryUtil.append(ldapEntry.getName(), proxyBaseDn);

                    if (debug) log.debug("Renaming \""+dn+"\"");

                    dn = convertDn(dn, proxyBaseDn, proxyDn);
                    if (debug) log.debug("into "+dn);

                    Attributes attributes = new Attributes();
                    for (NamingEnumeration i=ldapEntry.getAttributes().getAll(); i.hasMore(); ) {
                        javax.naming.directory.Attribute attribute = (javax.naming.directory.Attribute)i.next();
                        String name = attribute.getID();

                        for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                            Object value = j.next();
                            attributes.addValue(name, value);
                        }
                    }

                    Entry entry = new Entry(dn, entryMapping, attributes);
                    response.add(entry);
                }

                public void close() throws Exception {
                    if (debug) log.debug("Closing SearchResult to Entry conversion pipeline.");
                    closeClient(session, partition, sourceConfig, client);
                }
            };

            //connector.search(partition, sourceConfig, null, filter, newRequest, sr);
            client.search(targetDn.toString(), filter.toString(), newRequest, sr);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            response.close();
        }
    }
}
