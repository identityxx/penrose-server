package org.safehaus.penrose.jdbc;

import org.apache.commons.dbcp.ConnectionFactory;
import org.safehaus.penrose.jdbc.connection.JDBCConnection;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * @author Endi Sukma Dewata
 */
public class JDBCConnectionFactory implements ConnectionFactory {

    public Driver driver;
    public Properties properties = new Properties();

    public String url;
    public int queryTimeout;
    public String quote;

    public JDBCConnectionFactory(
            Map<String,String> parameters
    ) throws Exception {

        String driverClass = parameters.get(JDBCConnection.DRIVER);
        Class clazz = Class.forName(driverClass);
        driver = (Driver)clazz.newInstance();

        parseParameters(parameters);
        init();
    }

    public JDBCConnectionFactory(
            ClassLoader cl,
            Map<String,String> parameters
    ) throws Exception {

        String driverClass = parameters.get(JDBCConnection.DRIVER);
        Class clazz = cl.loadClass(driverClass);
        driver = (Driver)clazz.newInstance();

        parseParameters(parameters);
        init();
    }

    public JDBCConnectionFactory(
            Driver driver,
            Map<String,String> parameters
    ) throws Exception {

        this.driver = driver;

        parseParameters(parameters);
        init();
    }

    public void parseParameters(Map<String,String> parameters) {

        properties.putAll(parameters);

        url = parameters.remove(JDBCConnection.URL);

        String s = parameters.remove(JDBCConnection.QUERY_TIMEOUT);
        queryTimeout = s == null ? 0 : Integer.parseInt(s);

        quote = parameters.remove(JDBCConnection.QUOTE);
    }

    public void init() throws Exception {
    }

    public Connection createConnection() throws SQLException {
        return driver.connect(url, properties);
    }

    public String quote(String s) {
        StringBuilder sb = new StringBuilder();
        if (quote != null) sb.append(quote);
        sb.append(s);
        if (quote != null) sb.append(quote);
        return sb.toString();
    }

    public Driver getDriver() {
        return driver;
    }

    public Properties getProperties() {
        return properties;
    }

    public String getUrl() {
        return url;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    public String getQuote() {
        return quote;
    }
}
