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
package org.safehaus.penrose.adapter.ldap;

import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.ldap.Attribute;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.source.ldap.LDAPSourceSync;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connection.ConnectionManager;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class LDAPAdapter extends Adapter {

    public final static String BASE_DN        = "baseDn";
    public final static String SCOPE          = "scope";
    public final static String FILTER         = "filter";
    public final static String OBJECT_CLASSES = "objectClasses";
    public final static String SIZE_LIMIT     = "sizeLimit";
    public final static String TIME_LIMIT     = "timeLimit";

    public final static String PAGE_SIZE      = "pageSize";
    public final static int DEFAULT_PAGE_SIZE = 1000;

    public final static String AUTHENTICATION          = "authentication";
    public final static String AUTHENTICATION_DEFAULT  = "default";
    public final static String AUTHENTICATION_FULL     = "full";
    public final static String AUTHENTICATION_DISABLED = "disabled";

    public void init() throws Exception {
    }

    public Object openConnection() throws Exception {
        return new LDAPClient(getParameters());
    }

    public String getSyncClassName() {
        return LDAPSourceSync.class.getName();
    }

    public LDAPClient createClient(Session session, Partition partition, Source source) throws Exception {

        String authentication = source.getParameter(AUTHENTICATION);
        //if (debug) log.debug("Authentication: "+authentication);

        if (AUTHENTICATION_DISABLED.equals(authentication)) {
            if (debug) log.debug("Pass-Through Authentication is disabled.");
            throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
        }

        String connectionName = source.getConnectionName();
        ConnectionManager connectionManager = penroseContext.getConnectionManager();
        Connection connection = connectionManager.getConnection(partition, connectionName);

        return new LDAPClient(connection.getParameters());
    }

    public void storeClient(Session session, Partition partition, Source source, LDAPClient client) throws Exception {

        String authentication = source.getParameter(AUTHENTICATION);
        //if (debug) log.debug("Authentication: "+authentication);

        if (AUTHENTICATION_FULL.equals(authentication)) {
            if (debug) log.debug("Storing connection info in session.");

            String connectionName = source.getConnectionName();
            ConnectionManager connectionManager = penroseContext.getConnectionManager();
            Connection connection = connectionManager.getConnection(partition, connectionName);

            if (session != null) session.setAttribute(partition.getName()+".connection."+connection.getName(), client);
        } else {
            try { if (client != null) client.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    public LDAPClient getClient(Session session, Partition partition, Source source) throws Exception {

        String authentication = source.getParameter(AUTHENTICATION);
        if (debug) log.debug("Authentication: "+authentication);

        ConnectionManager connectionManager = penroseContext.getConnectionManager();
        Connection connection = connectionManager.getConnection(partition, source.getConnectionName());
        LDAPClient client;

        if (AUTHENTICATION_FULL.equals(authentication)) {
            if (debug) log.debug("Getting connection info from session.");

            client = session == null ? null : (LDAPClient)session.getAttribute(partition.getName()+".connection."+connection.getName());

            if (client == null) {

                if (session == null || session.isRootUser()) {
                    if (debug) log.debug("Creating new connection.");

                    client = new LDAPClient(connection.getParameters());

                } else {
                    if (debug) log.debug("Missing credentials.");
                    throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
                }
            }

        } else {
            if (debug) log.debug("Creating new connection.");

            client = new LDAPClient(connection.getParameters());
        }

        return client;
    }

    public void closeClient(Session session, Partition partition, Source source, LDAPClient client) throws Exception {

        String authentication = source.getParameter(AUTHENTICATION);
        //if (debug) log.debug("Authentication: "+authentication);

        if (!AUTHENTICATION_FULL.equals(authentication)) {
            try { if (client != null) client.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Storage
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void create(Source source) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Create "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        //client.createTable(source);
    }

    public void rename(Source source, String name) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Create "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        //client.renameTable(source, name);
    }

    public void drop(Source source) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Drop "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        //client.dropTable(source);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            Source source,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Add "+ source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = getClient(session, partition, source);

        try {
            String baseDn = source.getParameter(BASE_DN);

            DNBuilder db = new DNBuilder();
            db.append(request.getDn());
            db.append(baseDn);
            DN dn = db.toDn();

            String objectClasses = source.getParameter(OBJECT_CLASSES);
            Attribute ocAttribute = new Attribute("objectClass");
            for (StringTokenizer st = new StringTokenizer(objectClasses, ","); st.hasMoreTokens(); ) {
                String objectClass = st.nextToken().trim();
                ocAttribute.addValue(objectClass);
            }

            Attributes attributes = (Attributes)request.getAttributes().clone();
            attributes.set(ocAttribute);

            AddRequest newRequest = new AddRequest(request);
            newRequest.setDn(dn);
            newRequest.setAttributes(attributes);

            if (debug) log.debug("Adding entry "+dn);

            client.add(newRequest, response);

            log.debug("Add operation completed.");

        } finally {
            closeClient(session, partition, source, client);
        }
     }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            Source source,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Bind "+ source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = createClient(session, partition, source);

        try {
            String baseDn = source.getParameter(BASE_DN);

            DNBuilder db = new DNBuilder();
            db.append(request.getDn());
            db.append(baseDn);
            DN dn = db.toDn();

            BindRequest newRequest = new BindRequest(request);
            newRequest.setDn(dn);

            if (debug) log.debug("Binding as "+dn);

            client.bind(newRequest, response);

            log.debug("Bind operation completed.");

        } finally {
            storeClient(session, partition, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean compare(
            Session session,
            Source source,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Compare "+ source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = getClient(session, partition, source);

        try {
            String baseDn = source.getParameter(BASE_DN);

            DNBuilder db = new DNBuilder();
            db.append(request.getDn());
            db.append(baseDn);
            DN dn = db.toDn();

            CompareRequest newRequest = (CompareRequest)request.clone();
            newRequest.setDn(dn);

            if (debug) log.debug("Comparing entry "+dn);

            boolean b = client.compare(newRequest, response);

            log.debug("Compare operation completed ["+b+"].");

            return b;

        } finally {
            closeClient(session, partition, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Source source,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Delete "+ source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = getClient(session, partition, source);

        try {
            String baseDn = source.getParameter(BASE_DN);

            DNBuilder db = new DNBuilder();
            db.append(request.getDn());
            db.append(baseDn);
            DN dn = db.toDn();

            DeleteRequest newRequest = new DeleteRequest(request);
            newRequest.setDn(dn);

            if (debug) log.debug("Deleting entry "+dn);

            client.delete(newRequest, response);

            log.debug("Delete operation completed.");

        } finally {
            closeClient(session, partition, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            Source source,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Modify "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = getClient(session, partition, source);

        try {
            String baseDn = source.getParameter(BASE_DN);

            DNBuilder db = new DNBuilder();
            db.append(request.getDn());
            db.append(baseDn);
            DN dn = db.toDn();

            ModifyRequest newRequest = new ModifyRequest(request);
            newRequest.setDn(dn);

            if (debug) log.debug("Modifying entry "+dn);

            client.modify(newRequest, response);

            log.debug("Modify operation completed.");

        } finally {
            closeClient(session, partition, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            Source source,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ModRdn "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = getClient(session, partition, source);

        try {
            String baseDn = source.getParameter(BASE_DN);

            DNBuilder db = new DNBuilder();
            db.append(request.getDn());
            db.append(baseDn);
            DN dn = db.toDn();

            ModRdnRequest newRequest = new ModRdnRequest(request);
            newRequest.setDn(dn);

            if (debug) log.debug("Renaming entry "+dn);

            client.modrdn(newRequest, response);

            log.debug("ModRdn operation completed.");

        } finally {
            closeClient(session, partition, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            final Session session,
            final Source source,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        final LDAPClient client = getClient(session, partition, source);

        try {
            response.setSizeLimit(request.getSizeLimit());

            DN dn = new DN(source.getParameter(BASE_DN));

            SearchRequest newRequest = (SearchRequest)request.clone();
            newRequest.setDn(dn);

            String scope = source.getParameter(SCOPE);
            if ("OBJECT".equals(scope)) {
                newRequest.setScope(SearchRequest.SCOPE_BASE);

            } else if ("ONELEVEL".equals(scope)) {
                newRequest.setScope(SearchRequest.SCOPE_ONE);

            } else if ("SUBTREE".equals(scope)) {
                newRequest.setScope(SearchRequest.SCOPE_SUB);
            }

            String filter = source.getParameter(FILTER);
            if (filter != null) {
                Filter f1 = request.getFilter();
                Filter f2 = FilterTool.parseFilter(filter);
                f1 = FilterTool.appendAndFilter(f1, f2);
                newRequest.setFilter(f1);
            }

            String sizeLimit = source.getParameter(SIZE_LIMIT);
            if (sizeLimit != null) {
                newRequest.setSizeLimit(Long.parseLong(sizeLimit));
            }

            String timeLimit = source.getParameter(TIME_LIMIT);
            if (timeLimit != null) {
                newRequest.setTimeLimit(Long.parseLong(timeLimit));
            }

            SearchResponse<SearchResult> newResponse = new SearchResponse<SearchResult>() {
                public void add(SearchResult sr) throws Exception {

                    SearchResult searchResult = createSearchResult(source, sr);
                    if (searchResult == null) return;

                    if (debug) {
                        searchResult.print();
                    }

                    response.add(searchResult);
                }
            };

            if (debug) log.debug("Searching with base "+dn);

            client.search(newRequest, newResponse);

            log.debug("Search operation completed.");

        } finally {
            response.close();
            closeClient(session, partition, source, client);
        }
    }

    public SearchResult createSearchResult(
            Source source,
            SearchResult sr
    ) throws Exception {

        DN dn = sr.getDn();
        RDN rdn = dn.getRdn();

        Attributes attributes = sr.getAttributes();

        DN newDn = new DN(rdn);
        Attributes newAttributes;

        if (source.getFields().isEmpty()) {
            newAttributes = (Attributes)attributes.clone();

        } else {
            newAttributes = new Attributes();

            for (String name : rdn.getNames()) {
                Object value = rdn.get(name);
                newAttributes.addValue("primaryKey." + name, value);
            }

            for (Field field : source.getFields()) {

                Attribute attr = attributes.get(field.getOriginalName());
                if (attr == null) {
                    if (field.isPrimaryKey()) return null;
                    continue;
                }

                String fieldName = field.getName();
                //if (fieldName.equals("objectClass")) continue;

                for (Object value : attr.getValues()) {
                    newAttributes.addValue(fieldName, value);
                }
            }
        }

        return new SearchResult(newDn, newAttributes);
    }
}
