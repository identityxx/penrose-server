package org.safehaus.penrose.jdbc;

import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.ObjectPool;

import javax.sql.DataSource;


/**
 * @author Endi Sukma Dewata
 */
public class JDBCPoolableClient extends JDBCClient {

    public DataSource ds;

    public JDBCPoolableClient(
            ObjectPool connectionPool,
            JDBCConnectionFactory connectionFactory
    ) throws Exception {
        super(connectionFactory);

        ds = new PoolingDataSource(connectionPool);
        
        // unsupported
        // ds.setLoginTimeout(loginTimeout);
    }

    public synchronized void connect() throws Exception {

        if (connection == null || connection.isClosed()) {
            log.debug("Getting JDBC connection from connection pool.");
            connection = ds.getConnection();
        }
    }

    public synchronized void close() throws Exception {
        log.debug("Returning JDBC connection to connection pool.");
        super.close();
    }
}
