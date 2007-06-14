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

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class LDAPAdapter extends Adapter {

    public final static String BASE_DN        = "baseDn";
    public final static String SCOPE          = "scope";
    public final static String FILTER         = "filter";
    public final static String OBJECT_CLASSES = "objectClasses";

    public final static String PAGE_SIZE      = "pageSize";
    public final static int DEFAULT_PAGE_SIZE = 1000;

    private LDAPClient client;

    public void init() throws Exception {
        client = new LDAPClient(getParameters());
    }

    public Object openConnection() throws Exception {
        return new LDAPClient(client, getParameters());
    }

    public String getSyncClassName() {
        return LDAPSourceSync.class.getName();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Storage
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void create(Source source) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Create "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        //client.createTable(source);
    }

    public void rename(Source source, String name) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Create "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        //client.renameTable(source, name);
    }

    public void drop(Source source) throws Exception {

        boolean debug = log.isDebugEnabled();

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
            Source source,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Add "+ source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        String baseDn = source.getParameter(LDAPAdapter.BASE_DN);

        DNBuilder db = new DNBuilder();
        db.append(request.getDn());
        db.append(baseDn);
        DN dn = db.toDn();

        String objectClasses = source.getParameter(LDAPAdapter.OBJECT_CLASSES);
        Attribute ocAttribute = new Attribute("objectClass");
        for (StringTokenizer st = new StringTokenizer(objectClasses, ","); st.hasMoreTokens(); ) {
            String objectClass = st.nextToken().trim();
            ocAttribute.addValue(objectClass);
        }

        Attributes attributes = new Attributes(request.getAttributes());
        attributes.add(ocAttribute);

        AddRequest newRequest = new AddRequest(request);
        newRequest.setDn(dn);
        newRequest.setAttributes(attributes);

        if (debug) log.debug("Adding entry "+dn);

        client.add(newRequest, response);

        log.debug("Add operation completed.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Source source,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Bind "+ source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        String baseDn = source.getParameter(LDAPAdapter.BASE_DN);

        DNBuilder db = new DNBuilder();
        db.append(request.getDn());
        db.append(baseDn);
        DN dn = db.toDn();

        BindRequest newRequest = new BindRequest(request);
        newRequest.setDn(dn);

        if (debug) log.debug("Binding as "+dn);

        client.bind(newRequest, response);

        log.debug("Bind operation completed.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Source source,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Delete "+ source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        String baseDn = source.getParameter(LDAPAdapter.BASE_DN);

        DNBuilder db = new DNBuilder();
        db.append(request.getDn());
        db.append(baseDn);
        DN dn = db.toDn();

        DeleteRequest newRequest = new DeleteRequest(request);
        newRequest.setDn(dn);

        if (debug) log.debug("Deleting entry "+dn);

        client.delete(newRequest, response);

        log.debug("Delete operation completed.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Source source,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Modify "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        String baseDn = source.getParameter(LDAPAdapter.BASE_DN);

        DNBuilder db = new DNBuilder();
        db.append(request.getDn());
        db.append(baseDn);
        DN dn = db.toDn();

        ModifyRequest newRequest = new ModifyRequest(request);
        newRequest.setDn(dn);
        
        if (debug) log.debug("Modifying entry "+dn);

        client.modify(newRequest, response);

        log.debug("Modify operation completed.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Source source,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ModRdn "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        String baseDn = source.getParameter(LDAPAdapter.BASE_DN);

        DNBuilder db = new DNBuilder();
        db.append(request.getDn());
        db.append(baseDn);
        DN dn = db.toDn();

        ModRdnRequest newRequest = new ModRdnRequest(request);
        newRequest.setDn(dn);

        if (debug) log.debug("Renaming entry "+dn);

        client.modrdn(newRequest, response);

        log.debug("ModRdn operation completed.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            final Source source,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        final boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        response.setSizeLimit(request.getSizeLimit());

        DNBuilder db = new DNBuilder();
        db.append(request.getDn());
        db.append(source.getParameter(LDAPAdapter.BASE_DN));
        DN dn = db.toDn();

        SearchRequest newRequest = new SearchRequest(request);
        newRequest.setDn(dn);

        String scope = source.getParameter(LDAPAdapter.SCOPE);
        if ("OBJECT".equals(scope)) {
            newRequest.setScope(SearchRequest.SCOPE_BASE);

        } else if ("ONELEVEL".equals(scope)) {
            newRequest.setScope(SearchRequest.SCOPE_ONE);

        } else if ("SUBTREE".equals(scope)) {
            newRequest.setScope(SearchRequest.SCOPE_SUB);
        }

        String filter = source.getParameter(LDAPAdapter.FILTER);
        if (filter != null) {
            Filter f1 = request.getFilter();
            Filter f2 = FilterTool.parseFilter(filter);
            f1 = FilterTool.appendAndFilter(f1, f2);
            newRequest.setFilter(f1);
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
            public void close() throws Exception {
                response.close();
            }
        };

        if (debug) log.debug("Searching with base "+dn);

        client.search(newRequest, newResponse);

        log.debug("Search operation completed.");
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
            newAttributes = new Attributes(attributes);

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
