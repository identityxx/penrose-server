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

import org.safehaus.penrose.filter.Filter;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class InMemoryConnectorQueryCache extends ConnectorQueryCache {

    public Map queryMap = new TreeMap();
    public Map queryExpirationMap = new LinkedHashMap();

    public Collection search(Filter filter) throws Exception {

        String key = filter == null ? "" : filter.toString();

        Collection pks = (Collection)queryMap.get(key);
        Date date = (Date)queryExpirationMap.get(key);

        if (date == null || date.getTime() <= System.currentTimeMillis()) {
            queryMap.remove(key);
            queryExpirationMap.remove(key);
            pks = null;
        }

        //log.debug("Getting source filter cache: ["+key+"] => "+pks);

        return pks;
    }

    public void put(Filter filter, Collection pks) throws Exception {
        if (getSize() == 0) return;
        
        String key = filter == null ? "" : filter.toString();

        Object object = queryMap.get(key);

        while (object == null && queryMap.size() >= getSize()) {
            //log.debug("Trimming source filter cache ("+dataMap.size()+").");
            Object k = queryMap.keySet().iterator().next();
            queryMap.remove(k);
            queryExpirationMap.remove(k);
        }

        //log.debug("Storing source filter cache: ["+key+"] => "+pks);
        queryMap.put(key, pks);
        queryExpirationMap.put(key, new Date(System.currentTimeMillis() + getExpiration() * 60 * 1000));
    }

    public void invalidate() throws Exception {
        queryMap.clear();
        queryExpirationMap.clear();
    }
}
