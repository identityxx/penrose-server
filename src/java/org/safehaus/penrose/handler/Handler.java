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
package org.safehaus.penrose.handler;

import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.*;
import org.safehaus.penrose.mapping.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPEntry;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Handler {

    Logger log = LoggerFactory.getLogger(getClass());

    private AddHandler addHandler;
    private BindHandler bindHandler;
    private CompareHandler compareHandler;
    private DeleteHandler deleteHandler;
    private ModifyHandler modifyHandler;
    private ModRdnHandler modRdnHandler;
    private SearchHandler searchHandler;

    private HandlerContext handlerContext;

    public Handler() {        
    }

    /**
     * Initialize the engine with a Penrose instance
     *
     * @param handlerContext
     * @throws Exception
     */
    public Handler(HandlerContext handlerContext) throws Exception {
        this.handlerContext = handlerContext;

        addHandler = new AddHandler(this);
        bindHandler = new BindHandler(this);
        compareHandler = new CompareHandler(this);
        deleteHandler = new DeleteHandler(this);
        modifyHandler = new ModifyHandler(this);
        modRdnHandler = new ModRdnHandler(this);
        searchHandler = new SearchHandler(this);
    }

    public int add(PenroseConnection connection, LDAPEntry entry) throws Exception {
        return getAddHandler().add(connection, entry);
    }

    public int bind(PenroseConnection connection, String dn, String password) throws Exception {
        return getBindHandler().bind(connection, dn, password);
    }

    public int compare(PenroseConnection connection, String dn, String attributeName,
            String attributeValue) throws Exception {

        return getCompareHandler().compare(connection, dn, attributeName, attributeValue);
    }

    public int unbind(PenroseConnection connection) throws Exception {
        return getBindHandler().unbind(connection);
    }

    public int delete(PenroseConnection connection, String dn) throws Exception {
        return getDeleteHandler().delete(connection, dn);
    }

    public int modify(PenroseConnection connection, String dn, List modifications) throws Exception {
        return getModifyHandler().modify(connection, dn, modifications);
    }

    public int modrdn(PenroseConnection connection, String dn, String newRdn) throws Exception {
        return getModRdnHandler().modrdn(connection, dn, newRdn);
    }

    public SearchResults search(PenroseConnection connection, String base, int scope,
            int deref, String filter, Collection attributeNames)
            throws Exception {

        SearchResults results = new SearchResults();

        try {
            SearchThread searchRunnable = new SearchThread(getSearchHandler(),
                    connection, base, scope, deref, filter, attributeNames,
                    results);
            handlerContext.getEngine().execute(searchRunnable);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            results.setReturnCode(LDAPException.OPERATIONS_ERROR);
            results.close();
        }

        return results;
    }

    public BindHandler getBindHandler() {
        return bindHandler;
    }

    public void setBindHandler(BindHandler bindHandler) {
        this.bindHandler = bindHandler;
    }

    public SearchHandler getSearchHandler() {
        return searchHandler;
    }

    public void setSearchHandler(SearchHandler searchHandler) {
        this.searchHandler = searchHandler;
    }

    public AddHandler getAddHandler() {
        return addHandler;
    }

    public void setAddHandler(AddHandler addHandler) {
        this.addHandler = addHandler;
    }

    public ModifyHandler getModifyHandler() {
        return modifyHandler;
    }

    public void setModifyHandler(ModifyHandler modifyHandler) {
        this.modifyHandler = modifyHandler;
    }

    public DeleteHandler getDeleteHandler() {
        return deleteHandler;
    }

    public void setDeleteHandler(DeleteHandler deleteHandler) {
        this.deleteHandler = deleteHandler;
    }

    public CompareHandler getCompareHandler() {
        return compareHandler;
    }

    public void setCompareHandler(CompareHandler compareHandler) {
        this.compareHandler = compareHandler;
    }

    public ModRdnHandler getModRdnHandler() {
        return modRdnHandler;
    }

    public void setModRdnHandler(ModRdnHandler modRdnHandler) {
        this.modRdnHandler = modRdnHandler;
    }

    public HandlerContext getHandlerContext() {
        return handlerContext;
    }

    public void setHandlerContext(HandlerContext handlerContext) {
        this.handlerContext = handlerContext;
    }
}

