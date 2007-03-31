package org.safehaus.penrose.cache.jdbc;

import org.safehaus.penrose.changelog.ChangeLogUtil;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.cache.CacheModule;

/**
 * @author Endi S. Dewata
 */
public class JDBCCacheModule extends CacheModule {

    public ChangeLogUtil createChangeLogUtil() throws Exception {
        return new JDBCChangeLogUtil();
    }
}
