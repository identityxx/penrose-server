package org.safehaus.penrose.test.sql;

import junit.framework.TestCase;
import org.safehaus.penrose.sql.SQLParser;
import org.safehaus.penrose.sql.Token;
import org.safehaus.penrose.sql.SQLSelect;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Arrays;

/**
 * @author Endi S. Dewata
 */
public class SQLParserTest extends TestCase {

    public Collection parse(String sql) throws Exception {
        StringReader reader = new StringReader(sql);
        SQLParser parser = new SQLParser(reader);

        Collection tokens = new ArrayList();
        Token token = parser.getNextToken();
        while (token != null && !"".equals(token.image)) {
            System.out.println("Token: ["+token+"]");
            tokens.add(token.image);
            token = parser.getNextToken();
        }

        return tokens;
    }

    public void testParsingSimpleQuery() throws Exception {
        Collection list1 = Arrays.asList(new String[] { "select", "*", "from", "table" });
        Collection list2 = parse("select * from table");

        assertEquals(list1, list2);
    }

    public void testParsingSimpleQueryWithWhereClause() throws Exception {
        Collection list1 = Arrays.asList(new String[] { "select", "*", "from", "table", "where", "column", "=", "?" });
        Collection list2 = parse("select * from table where column = ?");

        assertEquals(list1, list2);
    }

    public void testParsingJoinQuery() throws Exception {
        Collection list1 = Arrays.asList(new String[] { "select", "*", "from", "t1", "table1", ",", "t2", "table2" });
        Collection list2 = parse("select * from t1 table1, t2 table2");

        assertEquals(list1, list2);
    }

    public void testParsingSelectStatement() throws Exception {
        StringReader reader = new StringReader("select * from table");
        SQLParser parser = new SQLParser(reader);
        SQLSelect select = parser.SQLSelect();
        System.out.println(select.toString());
    }
}
