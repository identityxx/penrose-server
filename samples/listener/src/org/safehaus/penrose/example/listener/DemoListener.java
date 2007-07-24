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
            SearchResult searchResult = response.next();
            System.out.println(toString(searchResult));
        }

        session.unbind();

        session.close();

        penrose.stop();
    }

    public String toString(SearchResult result) throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("dn: ");
        sb.append(result.getDn());
        sb.append("\n");

        Attributes attributes = result.getAttributes();
        for (Attribute attribute : attributes.getAll()) {
            String name = attribute.getName();

            for (Object value : attribute.getValues()) {
                sb.append(name);
                sb.append(": ");
                sb.append(value);
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    public void beforeSearch(SearchEvent event) throws Exception {
        SearchRequest request = event.getRequest();
        System.out.println("#### Searching "+request.getDn()+" with filter "+request.getFilter()+".");

        if (request.getFilter().toString().equalsIgnoreCase("(ou=*)")) {
            throw LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
        }

        if (request.getFilter().toString().equalsIgnoreCase("(ou=secret)")) {
            throw LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
        }

        SearchResponse<SearchResult> response = event.getResponse();

        response.addListener(new SearchResponseAdapter() {
            public void postAdd(SearchResponseEvent event) {
                SearchResult result = (SearchResult)event.getObject();
                System.out.println("#### Returning "+result.getDn());
            }
        });
    }

    public void afterSearch(SearchEvent event) throws Exception {
        SearchResponse response = event.getResponse();
        int rc = response.getReturnCode();
        if (rc == LDAP.SUCCESS) {
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
