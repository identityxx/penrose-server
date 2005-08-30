/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
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
	
	public final static String SUFFIX = "dc=example,dc=com";

	public Penrose penrose;
    public PenroseConnection connection;

    public Demo(String s) throws Exception {
        super(s);
    }

	public void setUp() throws Exception {
		penrose = new Penrose();
		penrose.init();

        connection = penrose.openConnection();
	}

    public void tearDown() throws Exception {
        connection.close();
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
                "(cn=James Bond)",
                attributeNames);

        for (Iterator i = results.iterator(); i.hasNext();) {
            LDAPEntry entry = (LDAPEntry) i.next();
            System.out.println(toString(entry));
        }
    }

    public void testSearchWithSnInFilter() throws Throwable {
        ArrayList attributeNames = new ArrayList();

        SearchResults results = connection.search(
                SUFFIX,
                LDAPConnection.SCOPE_SUB,
                LDAPSearchConstraints.DEREF_ALWAYS,
                "(sn=Bond)",
                attributeNames);

        for (Iterator i = results.iterator(); i.hasNext();) {
            LDAPEntry entry = (LDAPEntry) i.next();
            System.out.println(toString(entry));
        }
    }

    public void testSearchWithUniqueMemberInFilter() throws Throwable {
        ArrayList attributeNames = new ArrayList();

        SearchResults results = connection.search(
                SUFFIX,
                LDAPConnection.SCOPE_SUB,
                LDAPSearchConstraints.DEREF_ALWAYS,
                "(uniqueMember=uid=jbond,ou=users,dc=example,dc=com)",
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
