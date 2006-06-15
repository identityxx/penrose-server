package org.safehaus.penrose.db;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.io.BufferedReader;
import java.io.FileReader;

/**
 * @author Endi S. Dewata
 */
public class InitializeDB {

    public void run() throws Exception {
        Class.forName("org.hsqldb.jdbcDriver");
        Connection c = DriverManager.getConnection("jdbc:hsqldb:file:samples/db/example", "sa", "");

        BufferedReader in = new BufferedReader(new FileReader("samples/sql/create.sql"));

        StringBuffer sb = new StringBuffer();
        String line;

        while ((line = in.readLine()) != null) {
            line = line.trim();

            int i;
            while ((i = line.indexOf(";")) >= 0) {
                if (sb.length() > 0) sb.append(" ");
                sb.append(line.substring(0, i));

                String sql = sb.toString();
                System.out.println("Executing "+sql);

                PreparedStatement ps = null;

                try {
                    ps = c.prepareStatement(sql);
                    ps.execute();

                } catch (Exception e) {
                    System.err.println("ERROR: "+e.getMessage());

                } finally {
                    if (ps != null) try { ps.close(); } catch (Exception e) {}
                }

                sb.setLength(0);
                line = line.substring(i+1);
            }

            if (sb.length() > 0) sb.append(" ");
            sb.append(line);
        }

        in.close();
    }

    public static void main(String args[]) throws Exception {
        InitializeDB main = new InitializeDB();
        main.run();
    }
}
