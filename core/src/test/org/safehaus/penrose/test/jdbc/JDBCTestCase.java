package org.safehaus.penrose.test.jdbc;

import junit.framework.TestCase;
import org.safehaus.penrose.Penrose;

import java.sql.*;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class JDBCTestCase extends TestCase {

    public String driver = "org.hsqldb.jdbcDriver";
    public String url = "jdbc:hsqldb:mem:penrose";
    public String user = "sa";
    public String password = "";

    public JDBCTestCase() throws Exception {
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

    public Collection executeQuery(String sql) throws Exception {
        return executeQuery(sql, new ArrayList());
    }

    public Collection executeQuery(String sql, Collection parameters) throws Exception {
        Collection results = new ArrayList();

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = getConnection();
            ps = con.prepareStatement(sql);
            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); ) {
                Object parameter = i.next();
                ps.setObject(counter++, parameter);
            }
            rs = ps.executeQuery();
            ResultSetMetaData md = rs.getMetaData();

            while (rs.next()) {
                Map row = new LinkedHashMap();
                for (int i=1; i<=md.getColumnCount(); i++) {
                    String name = md.getColumnName(i);
                    Object value = rs.getObject(i);
                    row.put(name, value);
                }
                results.add(row);
            }

            return results;

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
    
    public void testDummy()
    {
    	assertEquals("a", "a");
    }
}
