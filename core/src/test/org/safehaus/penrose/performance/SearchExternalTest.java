package org.safehaus.penrose.performance;

import junit.framework.TestCase;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.*;
import java.util.Date;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Hashtable;
import java.text.NumberFormat;
import java.text.DecimalFormat;

/**
 * @author Endi S. Dewata
 */
public class SearchExternalTest extends TestCase {

    public void testSearchRate() throws Exception {
        final Collection list = new ArrayList();
        int count = 1000;

        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, "ldap://localhost:10389/");
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, "uid=admin,ou=system");
        env.put(Context.SECURITY_CREDENTIALS, "secret");

        DirContext ctx = new InitialDirContext(env);

        SearchControls sc = new SearchControls();
        sc.setSearchScope(SearchControls.OBJECT_SCOPE);

        list.add(new Date());

        for (int i=0; i<count; i++) {
            System.out.println("Search "+(i+1)+" of "+count);

            NamingEnumeration e = ctx.search("uid=jstockton,ou=Users,dc=JDBC,dc=Example,dc=com", "(objectClass=*)", sc);

            while (e.hasMore()) e.next();

            list.add(new Date());
        }

        ctx.close();

        Date times[] = new Date[list.size()];
        times = (Date[])list.toArray(times);

        double total = (double)(times[count].getTime() - times[0].getTime()) / 1000;
        double rate = (double)count/(double)total;

        NumberFormat nf = new DecimalFormat("0.00");

        System.out.println("Start time : "+times[0]);
        System.out.println("End time   : "+times[count]);
        System.out.println("Total time : "+nf.format(total)+" s");
        System.out.println("Rate       : "+nf.format(rate)+" operations/s");
    }
}
