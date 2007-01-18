package org.safehaus.penrose.test.quick.jdbc;

import org.apache.log4j.*;
import org.safehaus.penrose.Penrose;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;

import junit.framework.TestCase;

/**
 * @author Endi S. Dewata
 */
public class JDBCTestCase extends TestCase {

    String driver = "org.hsqldb.jdbcDriver";
    String url = "jdbc:hsqldb:mem:penrose";
    String user = "sa";
    String password = "";

    Penrose penrose;

    public JDBCTestCase() throws Exception {
        PatternLayout patternLayout = new PatternLayout("%-20C{1} [%4L] %m%n");

        ConsoleAppender appender = new ConsoleAppender(patternLayout);
        BasicConfigurator.configure(appender);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        logger.setLevel(Level.DEBUG);
        logger.setAdditivity(false);

        Class.forName(driver);
    }

    public Connection getConnection() throws Exception {
        return DriverManager.getConnection(url, user, password);
    }

    public int executeUpdate(String sql) throws Exception {
        return executeUpdate(sql, new ArrayList());
    }

    public int executeUpdate(String sql, Collection parameters) throws Exception {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = getConnection();
            ps = con.prepareStatement(sql);
            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); ) {
                Object parameter = i.next();
                ps.setObject(counter++, parameter);
            }
            return ps.executeUpdate();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public ResultSet executeQuery(String sql) throws Exception {
        return executeQuery(sql, new ArrayList());
    }

    public ResultSet executeQuery(String sql, Collection parameters) throws Exception {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = getConnection();
            ps = con.prepareStatement(sql);
            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); ) {
                Object parameter = i.next();
                ps.setObject(counter++, parameter);
            }
            return ps.executeQuery();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }
}
