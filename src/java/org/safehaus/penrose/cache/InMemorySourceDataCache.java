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

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class InMemorySourceDataCache extends SourceDataCache {

    Map dataMap = new TreeMap();
    Map uniqueKeys = new TreeMap();
    Map expirationMap = new LinkedHashMap();

    public Object get(Object key) throws Exception {
        Row normalizedKey = cacheContext.getSchema().normalize((Row)key);

        AttributeValues attributeValues = (AttributeValues)dataMap.get(normalizedKey);
        if (attributeValues == null) return null;

        Date date = (Date)expirationMap.get(normalizedKey);
        if (date == null || date.getTime() <= System.currentTimeMillis()) return null;

        return attributeValues;
    }

    public Map search(Collection keys, Collection missingKeys) throws Exception {

        Map results = new TreeMap();

        for (Iterator i=keys.iterator(); i.hasNext(); ) {
            Row key = (Row)i.next();
            Row normalizedKey = cacheContext.getSchema().normalize(key);

            AttributeValues attributeValues = (AttributeValues)get(normalizedKey);

            if (attributeValues == null) {
                attributeValues = (AttributeValues)uniqueKeys.get(normalizedKey);
            }

            if (attributeValues == null) {
                missingKeys.add(key);
                continue;
            }

            boolean found = false;
            for (Iterator j=dataMap.keySet().iterator(); j.hasNext(); ) {
                Row pk = (Row)j.next();
                attributeValues = (AttributeValues)dataMap.get(pk);

                Date date = (Date)expirationMap.get(pk);
                if (date == null || date.getTime() <= System.currentTimeMillis()) continue;

                boolean f = cacheContext.getSchema().partialMatch(attributeValues, normalizedKey);
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
        Row normalizedKey = cacheContext.getSchema().normalize((Row)key);
        AttributeValues values = (AttributeValues)object;

        while (dataMap.get(normalizedKey) == null && dataMap.size() >= size) {
            //log.debug("Trimming source data cache ("+dataMap.size()+").");
            Object k = expirationMap.keySet().iterator().next();
            dataMap.remove(k);
            expirationMap.remove(k);
        }

        //log.debug("Storing source data cache ("+dataMap.size()+"): "+normalizedKey);
        dataMap.put(normalizedKey, values);
        expirationMap.put(normalizedKey, new Date(System.currentTimeMillis() + expiration * 60 * 1000));

        for (Iterator j=sourceDefinition.getFieldDefinitions().iterator(); j.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)j.next();
            if (!fieldDefinition.isUnique()) continue;

            Object value = values.getOne(fieldDefinition.getName());

            Row uniqueKey = new Row();
            uniqueKey.set(fieldDefinition.getName(), value);
            Row normalizedUniqueKey = cacheContext.getSchema().normalize(uniqueKey);
            uniqueKeys.put(normalizedUniqueKey, values);
        }
    }

    public void remove(Object key) throws Exception {

        Row normalizedKey = cacheContext.getSchema().normalize((Row)key);

        //log.debug("Removing source data cache ("+dataMap.size()+"): "+normalizedKey);
        dataMap.remove(normalizedKey);
        expirationMap.remove(normalizedKey);

    }
}
