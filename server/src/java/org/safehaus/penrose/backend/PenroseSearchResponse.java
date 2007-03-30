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

import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.control.Control;

import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class PenroseSearchResponse
        extends PenroseResponse
        implements com.identyx.javabackend.SearchResponse {

    SearchResponse searchResponse;

    public PenroseSearchResponse(SearchResponse searchResponse) {
        super(searchResponse);
        this.searchResponse = searchResponse;
    }

    public Object next() throws Exception {
        SearchResult result = (SearchResult)searchResponse.next();

        PenroseEntry entry = new PenroseEntry(result.getEntry());

        Collection controls = new ArrayList();
        for (Iterator i= result.getControls().iterator(); i.hasNext(); ) {
            Control control = (Control)i.next();
            controls.add(new PenroseControl(control));
        }

        return new PenroseSearchResult(entry, controls);
    }

    public boolean hasNext() throws Exception {
        return searchResponse.hasNext();
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }
}
