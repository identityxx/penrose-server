/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.filter.Filter;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public interface EntryCache {

    public EntryCacheConfig getCacheConfig();
    public void setCacheConfig(EntryCacheConfig cacheConfig);

    public Collection getParameterNames();
    public String getParameter(String name);

    public void init(EntryCacheConfig cacheConfig, EntryCacheContext cacheContext) throws Exception;
    public void init() throws Exception;

    public Collection findPrimaryKeys(EntryDefinition entry, Filter filter) throws Exception;
    public Entry get(EntryDefinition entry, Row pk) throws Exception;
    public Map get(EntryDefinition entry, Collection pks) throws Exception;

    public void put(EntryDefinition entry, AttributeValues values, Date date) throws Exception;
    public void remove(EntryDefinition entry, AttributeValues values, Date date) throws Exception;

    public Date getModifyTime(EntryDefinition entry, Collection pks) throws Exception;
    public Date getModifyTime(EntryDefinition entry, Row pk) throws Exception;
}
