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
package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldapbackend.DN;
import org.safehaus.penrose.control.Control;
import org.safehaus.penrose.ldap.SearchListener;
import org.safehaus.penrose.ldap.SearchReference;
import org.safehaus.penrose.ldap.SearchReferenceException;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class PenroseSearchResponse
        extends PenroseResponse
        implements org.safehaus.penrose.ldapbackend.SearchResponse {

    public Logger log = LoggerFactory.getLogger(getClass());

    SearchResponse searchResponse;

    public PenroseSearchResponse(SearchResponse searchResponse) {
        super(searchResponse);
        this.searchResponse = searchResponse;
    }

    public PenroseSearchResult createSearchResult(SearchResult result) {

        DN dn = new PenroseDN(result.getDn());
        org.safehaus.penrose.ldapbackend.Attributes attributes = new PenroseAttributes(result.getAttributes());

        Collection<org.safehaus.penrose.ldapbackend.Control> controls = new ArrayList<org.safehaus.penrose.ldapbackend.Control>();
        for (Control control : result.getControls()) {
            controls.add(new PenroseControl(control));
        }

        return new PenroseSearchResult(dn, attributes, controls);
    }

    public PenroseSearchReference createSearchReference(SearchReference result) {

        DN dn = new PenroseDN(result.getDn());
        Collection<String> urls = result.getUrls();

        Collection<org.safehaus.penrose.ldapbackend.Control> controls = new ArrayList<org.safehaus.penrose.ldapbackend.Control>();
        for (Control control : result.getControls()) {
            controls.add(new PenroseControl(control));
        }

        return new PenroseSearchReference(dn, urls, controls);
    }

    public Object next() throws Exception {
        try {
            SearchResult result = searchResponse.next();
            return createSearchResult(result);

        } catch (SearchReferenceException e) {
            SearchReference reference = e.getReference();
            throw new PenroseSearchReferenceException(createSearchReference(reference));
        }
    }

    public void addListener(final org.safehaus.penrose.ldapbackend.SearchListener listener) throws Exception {
        searchResponse.addListener(new SearchListener() {
            public void add(SearchResult result) throws Exception {
                listener.add(createSearchResult(result));
            }
            public void add(SearchReference reference) throws Exception {
                listener.add(createSearchReference(reference));
            }
            public void close() throws Exception {
                listener.close();
            }
        });
    }

    public boolean hasNext() throws Exception {
        return searchResponse.hasNext();
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }
}
