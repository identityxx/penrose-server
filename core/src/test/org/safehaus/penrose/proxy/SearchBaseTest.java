package org.safehaus.penrose.proxy;

import junit.framework.TestCase;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Date;
import java.text.NumberFormat;
import java.text.DecimalFormat;

import org.safehaus.penrose.util.EntryUtil;

/**
 * @author Endi S. Dewata
 */
public class SearchBaseTest extends TestCase {

    ArrayList dns;
    Hashtable env;

    public void setUp() throws Exception {
        dns = new ArrayList();

        env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, "ldap://localhost:10389/");
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, "uid=swhite,ou=Users,dc=Proxy,dc=Example,dc=com");
        env.put(Context.SECURITY_CREDENTIALS, "swh1t3");

        DirContext ctx = new InitialDirContext(env);

        SearchControls sc = new SearchControls();
        sc.setSearchScope(SearchControls.ONELEVEL_SCOPE);

        String baseDn = "ou=Users,dc=Proxy,dc=Example,dc=com";
        System.out.println("Searching "+baseDn+":");

        NamingEnumeration e = ctx.search(baseDn, "(objectClass=*)", sc);

        while (e.hasMore()) {
            SearchResult sr = (SearchResult)e.next();
            String dn = EntryUtil.append(sr.getName(), baseDn);
            System.out.println(" - "+dn);
            dns.add(dn);
        }

        System.out.println("Found "+dns.size()+" entries.");

        ctx.close();
    }

    public void tearDown() throws Exception {
    }

    public void testSearchRate() throws Exception {

        Collection list = new ArrayList();
        int count = 100;
        int size = dns.size();

        SearchControls sc = new SearchControls();
        sc.setSearchScope(SearchControls.OBJECT_SCOPE);

        list.add(new Date());
        for (int i=0; i<count; i++) {
            String baseDn = (String)dns.get(i % size);

            System.out.println("Test search #"+(i+1)+": "+baseDn);

            DirContext ctx = new InitialDirContext(env);

            NamingEnumeration e = ctx.search(baseDn, "(cn=*te*)", sc);
            while (e.hasMore()) e.next();

            ctx.close();
            
            list.add(new Date());
        }

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
