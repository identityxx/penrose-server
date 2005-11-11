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
public class InMemoryConnectorDataCache extends ConnectorDataCache {

    int lastChangeNumber;

    public Map queryMap = new TreeMap();
    public Map queryExpirationMap = new LinkedHashMap();

    Map dataMap = new TreeMap();
    Map uniqueKeyMap = new TreeMap();
    Map dataExpirationMap = new LinkedHashMap();

    public Object get(Object key) throws Exception {

        AttributeValues attributeValues = (AttributeValues)dataMap.get(key);
        if (attributeValues == null) return null;

        Date date = (Date)dataExpirationMap.get(key);
        if (date == null || date.getTime() <= System.currentTimeMillis()) return null;

        return attributeValues;
    }

    public Map getExpired() throws Exception {
        Map results = new TreeMap();

        for (Iterator j=dataMap.keySet().iterator(); j.hasNext(); ) {
            Row pk = (Row)j.next();
            AttributeValues attributeValues = (AttributeValues)dataMap.get(pk);

            Date date = (Date)dataExpirationMap.get(pk);
            if (date != null && date.getTime() > System.currentTimeMillis()) continue;

            results.put(pk, attributeValues);
        }

        return results;
    }

    public Row normalize(Row row) throws Exception {

        Row newRow = new Row();

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

    public boolean isValid(AttributeValues av, Row row) throws Exception {

        for (Iterator i=row.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = av.get(name);
            Object value = row.get(name);

            if (values == null && value == null) {
                continue;

            } else if (values == null || value == null) {
                return false;

            } else {
                boolean found = false;
                for (Iterator j=values.iterator(); j.hasNext() && !found; ) {
                    Object v = j.next();
                    //log.debug("comparing ["+v+"] with ["+value+"]: "+v.toString().equalsIgnoreCase(value.toString()));
                    if (v.toString().equalsIgnoreCase(value.toString())) found = true;
                }
                if (!found) return false;
            }
        }

        return true;
    }

    public Map load(Collection keys, Collection missingKeys) throws Exception {

        Map results = new TreeMap();

        for (Iterator i=keys.iterator(); i.hasNext(); ) {
            Row key = (Row)i.next();

            AttributeValues attributeValues = (AttributeValues)get(key);

            Collection uniqueKeys = (Collection)uniqueKeyMap.get(key);
            if (uniqueKeys != null) {
                for (Iterator j=uniqueKeys.iterator(); j.hasNext(); ) {
                    Row uniqueKey = (Row)j.next();
                    attributeValues = (AttributeValues)get(uniqueKey);
                    if (attributeValues != null) break;
                }
            }

            if (attributeValues == null) {
                missingKeys.add(key);
                continue;
            }

            boolean found = false;
            for (Iterator j=dataMap.keySet().iterator(); j.hasNext(); ) {
                Row pk = (Row)j.next();
                attributeValues = (AttributeValues)dataMap.get(pk);

                Date date = (Date)dataExpirationMap.get(pk);
                if (date == null || date.getTime() <= System.currentTimeMillis()) continue;

                boolean f = isValid(attributeValues, key);
                if (!f) continue;

                results.put(pk, attributeValues);
                found = true;
            }

            if (!found) {
                missingKeys.add(key);
            }
        }

        return results;
    }

    public void put(Object key, Object object) throws Exception {
        if (size == 0) return;

        AttributeValues values = (AttributeValues)object;

        while (dataMap.get(key) == null && dataMap.size() >= size) {
            //log.debug("Trimming source data cache ("+dataMap.size()+").");
            Object k = dataExpirationMap.keySet().iterator().next();
            dataMap.remove(k);
            dataExpirationMap.remove(k);
        }

        //log.debug("Storing source data cache ("+dataMap.size()+"): "+key);
        dataMap.put(key, values);
        dataExpirationMap.put(key, new Date(System.currentTimeMillis() + expiration * 60 * 1000));

        Collection uniqueKeys = new ArrayList();
        for (Iterator j=sourceDefinition.getFieldDefinitions().iterator(); j.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)j.next();
            if (!fieldDefinition.isUnique()) continue;

            String fieldName = fieldDefinition.getName();
            Object value = values.getOne(fieldName);

            Row uniqueKey = new Row();
            uniqueKey.set(fieldName, value);

            Row normalizedUniqueKey = normalize(uniqueKey);
            dataMap.put(normalizedUniqueKey, values);

            uniqueKeys.add(normalizedUniqueKey);
        }

        uniqueKeyMap.put(key, uniqueKeys);
    }

    public void remove(Object key) throws Exception {

        //log.debug("Removing source data cache ("+dataMap.size()+"): "+key);
        dataMap.remove(key);
        dataExpirationMap.remove(key);

    }

    public int getLastChangeNumber() {
        return lastChangeNumber;
    }

    public void setLastChangeNumber(int lastChangeNumber) {
        this.lastChangeNumber = lastChangeNumber;
    }

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
