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
import org.safehaus.penrose.partition.SourceConfig;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class InMemoryEntryCacheStorage extends EntryCacheStorage {

    Map queryMap = new TreeMap();
    Map queryExpirationMap = new LinkedHashMap();

    Map dataMap = new TreeMap();
    Map dataExpirationMap = new LinkedHashMap();

    public Entry get(String dn) throws Exception {

        //log.debug("Getting entry cache ("+dataMap.size()+"): "+rdn);

        Entry entry = (Entry)dataMap.get(dn);
        Date date = (Date)dataExpirationMap.get(dn);

        if (date == null || date.getTime() <= System.currentTimeMillis()) {
            dataMap.remove(dn);
            dataExpirationMap.remove(dn);
            return null;
        }

        return entry;
    }

    public Map getExpired() throws Exception {
        Map results = new TreeMap();
        return results;
    }

    public void put(String dn, Entry entry) throws Exception {
        if (getSize() == 0) return;

        while (dataMap.get(dn) == null && dataMap.size() >= getSize()) {
            //log.debug("Trimming entry cache ("+dataMap.size()+").");
            Object k = dataExpirationMap.keySet().iterator().next();
            dataMap.remove(k);
            dataExpirationMap.remove(k);
        }

        //log.debug("Storing entry cache ("+dataMap.size()+"): "+rdn);
        dataMap.put(dn, entry);
        dataExpirationMap.put(dn, new Date(System.currentTimeMillis() + getExpiration() * 60 * 1000));

        invalidate();
    }

    public void remove(String dn) throws Exception {

        //log.debug("Removing entry cache ("+dataMap.size()+"): "+rdn);
        dataMap.remove(dn);
        dataExpirationMap.remove(dn);

        invalidate();
    }

    /**
     * @return DNs (Collection of String)
     */
    public Collection search(Filter filter, String parentDn) throws Exception {

        Collection results = new ArrayList();

        String key = filter == null ? "" : filter.toString();
        //log.debug("Getting entry filter cache ("+queryMap.size()+"): "+key);

        Collection dns = (Collection)queryMap.get(key);
        if (dns == null) return null;

        if (parentDn == null) {
            results.addAll(dns);
        } else {
            for (Iterator i=dns.iterator(); i.hasNext(); ) {
                String dn = (String)i.next();
                String pdn = Entry.getParentDn(dn);
                if (parentDn.equals(pdn)) results.add(dn);
            }
        }

        Date date = (Date)queryExpirationMap.get(key);

        if (date == null || date.getTime() <= System.currentTimeMillis()) {
            queryMap.remove(key);
            queryExpirationMap.remove(key);
            return null;
        }

        return results;
    }

    /**
     * @return DNs (Collection of Strings)
     */
    public Collection search(SourceConfig sourceConfig, Row filter) throws Exception {

        Collection results = new ArrayList();

        for (Iterator i=dataMap.keySet().iterator(); i.hasNext(); ) {
            String dn = (String)i.next();

            Date date = (Date)dataExpirationMap.get(dn);
            if (date == null || date.getTime() <= System.currentTimeMillis()) continue;

            Entry entry = (Entry)dataMap.get(dn);
            AttributeValues sv = entry.getSourceValues();
            if (!sv.contains(filter)) continue;

            results.add(dn);
        }

        return results;
    }

    public void add(Filter filter, String dn) throws Exception {
        String key = filter == null ? "" : filter.toString();

        Collection dns = (Collection)queryMap.remove(key);

        while (dns == null && queryMap.size() >= getSize()) {
            //log.debug("Trimming entry filter cache ("+queryMap.size()+").");
            Object k = queryExpirationMap.keySet().iterator().next();
            queryMap.remove(k);
            queryExpirationMap.remove(k);
        }

        if (dns == null) {
            dns = new TreeSet();
            queryMap.put(key, dns);
        }
        //log.debug("Storing entry filter cache ("+queryMap.size()+"): "+key);
        dns.add(dn);
        queryExpirationMap.put(key, new Date(System.currentTimeMillis() + getExpiration() * 60 * 1000));
    }

    public void put(Filter filter, Collection dns) throws Exception {
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
        queryMap.put(key, dns);
        queryExpirationMap.put(key, new Date(System.currentTimeMillis() + getExpiration() * 60 * 1000));
    }

    public void invalidate() throws Exception {
        queryMap.clear();
        queryExpirationMap.clear();
    }

}
