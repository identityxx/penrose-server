/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
import java.util.Iterator;
import java.util.ArrayList;

import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.SearchResults;
import org.ietf.ldap.*;

import junit.textui.TestRunner;
import junit.framework.TestSuite;
import junit.framework.TestCase;

/**
 * @author Endi S. Dewata
 */
public class Demo extends TestCase {
	
	public final static String SUFFIX = "dc=Example,dc=com";

	public Penrose penrose;
    public PenroseConnection connection;

    public Demo(String s) throws Exception {
        super(s);
    }

	public void setUp() throws Exception {
		penrose = new Penrose();
		penrose.start();

        connection = penrose.openConnection();
	}

    public void tearDown() throws Exception {
        connection.close();
        penrose.stop();
    }

    public void testBind() throws Throwable {
        connection.bind("uid=admin,ou=system", "secret");
        connection.unbind();
    }

	public void testSearchAll() throws Throwable {
		ArrayList attributeNames = new ArrayList();

        SearchResults results = connection.search(
                SUFFIX,
                LDAPConnection.SCOPE_SUB,
                LDAPSearchConstraints.DEREF_ALWAYS,
                "(objectClass=*)",
                attributeNames);

        for (Iterator i = results.iterator(); i.hasNext();) {
            LDAPEntry entry = (LDAPEntry) i.next();
			System.out.println(toString(entry));
		}
	}

    public void testSearchWithCnInFilter() throws Throwable {
        ArrayList attributeNames = new ArrayList();

        SearchResults results = connection.search(
                SUFFIX,
                LDAPConnection.SCOPE_SUB,
                LDAPSearchConstraints.DEREF_ALWAYS,
                "(cn=Wine)",
                attributeNames);

        for (Iterator i = results.iterator(); i.hasNext();) {
            LDAPEntry entry = (LDAPEntry) i.next();
            System.out.println(toString(entry));
        }
    }

    public String toString(LDAPEntry entry) throws Exception {

        StringBuffer sb = new StringBuffer();
        sb.append("dn: "+entry.getDN()+"\n");

        LDAPAttributeSet attributeSet = entry.getAttributeSet();
        for (Iterator i=attributeSet.iterator(); i.hasNext(); ) {
            LDAPAttribute attribute = (LDAPAttribute)i.next();

            String name = attribute.getName();

            String values[] = attribute.getStringValueArray();

            for (int j=0; j<values.length; j++) {
                sb.append(name+": "+values[j]+"\n");
            }
        }

        return sb.toString();
    }

    public static void main(String args[]) throws Throwable {
        if (args.length == 0) {
            TestSuite suite = new TestSuite(Demo.class);
            TestRunner.run(suite);

        } else {
            TestSuite suite = new TestSuite();
            for (int i = 0; i < args.length; i++) {
                suite.addTest(new Demo(args[i]));
            }
            TestRunner.run(suite);
        }
    }
}
