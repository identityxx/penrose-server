/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.filter.Filter;

import java.util.HashMap;
import java.util.Map;
import java.util.Collection;
import java.util.LinkedHashMap;

/**
 * @author Endi S. Dewata
 */
public class FilterCache {

    public LinkedHashMap map = new LinkedHashMap();

    public Collection get(EntryDefinition entryDefinition, Filter filter) throws Exception {
        Map key = new HashMap();
        key.put("dn", entryDefinition.getDn());
        key.put("filter", filter.toString());

        return (Collection)map.get(key);
    }

    public void put(EntryDefinition entryDefinition, Filter filter, Collection rdns) throws Exception {
        Map key = new HashMap();
        key.put("dn", entryDefinition.getDn());
        key.put("filter", filter.toString());

        map.put(key, rdns);

        if (map.size() > 20) {
            key = (Map)map.keySet().iterator().next();
            map.remove(key);
        }
    }
}
