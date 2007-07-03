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
package org.safehaus.penrose.pipeline;

import org.safehaus.penrose.ldap.SearchResponse;
import org.ietf.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class Pipeline extends SearchResponse {

    public SearchResponse parent;

    public Pipeline(SearchResponse parent) {
        this.parent = parent;
    }

    public void add(Object object) throws Exception {
        parent.add(object);
    }

    public long getTotalCount() throws Exception {
        return parent.getTotalCount();
    }

    public void setException(Exception e) {
        parent.setException(e);
    }

    public LDAPException getException() {
        return parent.getException();
    }

    public void close() throws Exception {
        // don't close parent
    }

    public SearchResponse getParent() {
        return parent;
    }

    public void setParent(SearchResponse parent) {
        this.parent = parent;
    }
}
