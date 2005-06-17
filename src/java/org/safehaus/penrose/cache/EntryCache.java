/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.filter.Filter;

import java.util.Collection;
import java.util.Date;
import java.sql.ResultSet;

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

    public Collection search(EntryDefinition entry, Collection primaryKeys) throws Exception;

    public void insert(EntryDefinition entry, AttributeValues values, Date date) throws Exception;
    public void insert(EntryDefinition entry, Row row, Date date) throws Exception;

    public void delete(EntryDefinition entry, AttributeValues values, Date date) throws Exception;
    public void delete(EntryDefinition entry, String filter, Date date) throws Exception;

    public Date getModifyTime(EntryDefinition entry, String filter) throws Exception;
    public Date getModifyTime(EntryDefinition entry) throws Exception;
    public void setModifyTime(EntryDefinition entry, Date date) throws Exception;

    public String getPkAttributeNames(EntryDefinition entry);

    public Collection searchPrimaryKeys(
            EntryDefinition entry,
            Filter filter)
            throws Exception;

    public Row getPk(EntryDefinition entry, ResultSet rs) throws Exception;

    public String getTableName(EntryDefinition entry, boolean temporary);

    public EntryCacheFilterTool getCacheFilterTool();

    public void setCacheFilterTool(EntryCacheFilterTool cacheFilterTool);
}
