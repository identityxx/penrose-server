/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache.impl;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.cache.Cache;
import org.safehaus.penrose.cache.CacheFilterTool;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.AttributeDefinition;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.*;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.util.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Connection;

/**
 * @author Endi S. Dewata
 */
public class DefaultCache extends Cache {

    private CacheFilterTool cacheFilterTool;

    public Map homes = new HashMap();

    private DataSource ds;

    public void init() throws Exception {

        cacheFilterTool = new CacheFilterTool(getCacheContext());

        String driver    = getParameter(CacheConfig.DRIVER);
        String url       = getParameter(CacheConfig.URL);
        String username  = getParameter(CacheConfig.USER);
        String password  = getParameter(CacheConfig.PASSWORD);

        log.debug("Driver    : " + driver);
        log.debug("Url       : " + url);
        log.debug("Username  : " + username);
        log.debug("Password  : " + password);

        Properties properties = new Properties();
        properties.put("driver", driver);
        properties.put("url", url);
        properties.put("user", username);
        properties.put("password", password);

        Class.forName(driver);

        GenericObjectPool connectionPool = new GenericObjectPool(null);
        //connectionPool.setMaxActive(0);

        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(
                url, properties);

        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(
                connectionFactory, connectionPool, null, // statement pool
                // factory
                null, // test query
                false, // read only
                true // auto commit
        );

        ds = new PoolingDataSource(connectionPool);
    }

    public DataSource getDs() {
        return ds;
    }

    public void setDs(DataSource ds) {
        this.ds = ds;
    }

    public CacheFilterTool getCacheFilterTool() {
        return cacheFilterTool;
    }

    public void setCacheFilterTool(CacheFilterTool cacheFilterTool) {
        this.cacheFilterTool = cacheFilterTool;
    }
}
