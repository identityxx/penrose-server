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
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.filter.Filter;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class InMemoryEntryCache extends EntryCache {

    Map queryMap = new TreeMap();
    Map queryExpirationMap = new LinkedHashMap();

    Map dataMap = new TreeMap();
    Map dataExpirationMap = new LinkedHashMap();

    public Row normalize(Row row) throws Exception {

        Row newRow = new Row();
        if (row == null) return newRow;

        for (Iterator i=row.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = row.get(name);

            if (value == null) continue;

            if (value instanceof String) {
                value = ((String)value).toLowerCase();
            }

            newRow.set(name, value);
        }

        return newRow;
    }

    public Entry get(Object key) throws Exception {

        Row rdn = normalize((Row)key);
        //log.debug("Getting entry cache ("+dataMap.size()+"): "+rdn);

        Entry entry = (Entry)dataMap.get(rdn);
        Date date = (Date)dataExpirationMap.get(rdn);

        if (date == null || date.getTime() <= System.currentTimeMillis()) {
            dataMap.remove(rdn);
            dataExpirationMap.remove(rdn);
            return null;
        }

        return entry;
    }

    public Map getExpired() throws Exception {
        Map results = new TreeMap();
        return results;
    }

    public void put(Object key, Object object) throws Exception {
        if (getSize() == 0) return;

        Row rdn = normalize((Row)key);

        while (dataMap.get(rdn) == null && dataMap.size() >= getSize()) {
            //log.debug("Trimming entry cache ("+dataMap.size()+").");
            Object k = dataExpirationMap.keySet().iterator().next();
            dataMap.remove(k);
            dataExpirationMap.remove(k);
        }

        //log.debug("Storing entry cache ("+dataMap.size()+"): "+rdn);
        dataMap.put(rdn, object);
        dataExpirationMap.put(rdn, new Date(System.currentTimeMillis() + getExpiration() * 60 * 1000));

        invalidate();
    }

    public void remove(Object key) throws Exception {
        Row rdn = normalize((Row)key);

        //log.debug("Removing entry cache ("+dataMap.size()+"): "+rdn);
        dataMap.remove(rdn);
        dataExpirationMap.remove(rdn);

        invalidate();
    }

    public Collection search(Filter filter) throws Exception {

        String key = filter == null ? "" : filter.toString();
        //log.debug("Getting entry filter cache ("+queryMap.size()+"): "+key);

        Collection rdns = (Collection)queryMap.get(key);
        Date date = (Date)queryExpirationMap.get(key);

        if (date == null || date.getTime() <= System.currentTimeMillis()) {
            queryMap.remove(key);
            queryExpirationMap.remove(key);
            return null;
        }

        return rdns;
    }

    public void put(Filter filter, Collection rdns) throws Exception {
        if (getSize() == 0) return;

        String key = filter == null ? "" : filter.toString();

        Object object = (Collection)queryMap.remove(key);

        while (object == null && queryMap.size() >= getSize()) {
            //log.debug("Trimming entry filter cache ("+queryMap.size()+").");
            Object k = queryExpirationMap.keySet().iterator().next();
            queryMap.remove(k);
            queryExpirationMap.remove(k);
        }

        //log.debug("Storing entry filter cache ("+queryMap.size()+"): "+key);
        queryMap.put(key, rdns);
        queryExpirationMap.put(key, new Date(System.currentTimeMillis() + getExpiration() * 60 * 1000));
    }

    public void invalidate() throws Exception {
        queryMap.clear();
        queryExpirationMap.clear();
    }

}
