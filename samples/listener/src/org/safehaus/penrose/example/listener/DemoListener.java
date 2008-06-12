package org.safehaus.penrose.example.listener;

import org.apache.log4j.*;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;
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

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        Penrose penrose = penroseFactory.createPenrose("../..");
        penrose.start();

        Session session = penrose.createSession();

        try {
            session.bind("uid=admin,ou=system", "secret");

            SearchRequest request = new SearchRequest();
            request.setDn(SUFFIX);

            SearchResponse response = new SearchResponse();
            response.addListener(this);

            session.search(request, response);

            while (response.hasNext()) {
                SearchResult searchResult = response.next();
                System.out.println(toString(searchResult));
            }

            session.unbind();

        } finally {
            session.close();
        }

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

    public void add(SearchResult result) throws Exception {
        System.out.println("#### Received "+result.getDn());
    }

    public void add(SearchReference reference) throws Exception {
        System.out.println("#### Received "+reference.getDn());
    }

    public void close() throws Exception {
        System.out.println("#### Search completed.");
    }

    public static void main(String args[]) throws Exception {
        DemoListener demo = new DemoListener();
        demo.run();
    }
}
