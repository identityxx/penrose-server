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
package org.safehaus.penrose.openldap;

import org.openldap.backend.Result;
import org.safehaus.penrose.SearchResults;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PenroseResult implements Result {

    private SearchResults results;

    public PenroseResult(SearchResults results) {
        this.results = results;
    }

    public Object next() {
        return results.next();
    }

    public void close() {
        results.close();
    }

    public Collection getAll() {
        return results.getAll();
    }

    public int size() {
        return results.size();
    }

    public synchronized Iterator iterator() {
        return results.iterator();
    }

    public int getReturnCode() {
        return results.getReturnCode();
    }

    public boolean hasNext() {
        return results.hasNext();
    }
}
