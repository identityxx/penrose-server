package org.safehaus.penrose.example.listener;

import org.apache.log4j.*;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;
import org.safehaus.penrose.event.SearchListener;
import org.safehaus.penrose.event.SearchEvent;
import org.safehaus.penrose.session.*;
import org.ietf.ldap.LDAPException;

import javax.naming.NoPermissionException;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class DemoListener implements SearchListener {

    public final static String SUFFIX = "dc=Example,dc=com";

    public void run() throws Exception {

        ClassLoader cl = getClass().getClassLoader();
        while (cl != null) {
            System.out.println("Class loader: "+cl.getClass());
            cl = cl.getParent();
        }

        ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
        BasicConfigurator.configure(appender);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        logger.setLevel(Level.DEBUG);

        PenroseConfig penroseConfig = new DefaultPenroseConfig();
        penroseConfig.setHome("../..");

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        Penrose penrose = penroseFactory.createPenrose(penroseConfig);
        penrose.start();

        Session session = penrose.newSession();
        session.addSearchListener(this);

        session.bind("uid=admin,ou=system", "secret");

        SearchResponse<SearchResult> response = session.search(DemoListener.SUFFIX, "(objectClass=*)");

        while (response.hasNext()) {
            SearchResult searchResult = (SearchResult) response.next();
            System.out.println(toString(searchResult));
        }

        session.unbind();

        session.close();

        penrose.stop();
    }

    public String toString(SearchResult result) throws Exception {

        StringBuffer sb = new StringBuffer();
        sb.append("dn: "+result.getDn()+"\n");

        Attributes attributes = result.getAttributes();
        for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();
            String name = attribute.getName();

            for (Iterator j=attribute.getValues().iterator(); j.hasNext(); ) {
                Object value = j.next();
                sb.append(name+": "+value+"\n");
            }
        }

        return sb.toString();
    }

    public boolean beforeSearch(SearchEvent event) throws Exception {
        SearchRequest request = event.getRequest();
        System.out.println("#### Searching "+request.getDn()+" with filter "+request.getFilter()+".");

        if (request.getFilter().toString().equalsIgnoreCase("(ou=*)")) {
            return false;
        }

        if (request.getFilter().toString().equalsIgnoreCase("(ou=secret)")) {
            throw new NoPermissionException();
        }

        SearchResponse<SearchResult> response = event.getResponse();

        response.addListener(new SearchResponseAdapter() {
            public void postAdd(SearchResponseEvent event) {
                SearchResult result = (SearchResult)event.getObject();
                System.out.println("#### Returning "+result.getDn());
            }
        });

        return true;
    }

    public void afterSearch(SearchEvent event) throws Exception {
        int rc = event.getReturnCode();
        if (rc == LDAPException.SUCCESS) {
            System.out.println("#### Search succeded.");
        } else {
            System.out.println("#### Search failed. RC="+rc);
        }
    }

    public static void main(String args[]) throws Exception {
        DemoListener demo = new DemoListener();
        demo.run();
    }
}
