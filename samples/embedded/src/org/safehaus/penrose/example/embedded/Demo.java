package org.safehaus.penrose.example.embedded;

import org.apache.log4j.*;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class Demo {

    Logger log = Logger.getLogger(getClass());

    public Demo() {
        ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
        BasicConfigurator.configure(appender);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        logger.setLevel(Level.WARN);
    }

    public EntryConfig createRootEntry() {

        EntryConfig entryConfig = new EntryConfig();
        entryConfig.setDn("dc=Example,dc=com");

        entryConfig.addObjectClass("dcObject");
        entryConfig.addObjectClass("organization");

        entryConfig.addAttributesFromRdn();

        entryConfig.addAttributeConfig("o", "Example");

        return entryConfig;
    }

    public EntryConfig createUsersEntry() {

        EntryConfig entryConfig = new EntryConfig();
        entryConfig.setDn("ou=Users,dc=Example,dc=com");

        entryConfig.addObjectClass("organizationalUnit");

        entryConfig.addAttributesFromRdn();

        return entryConfig;
    }

    public void run() throws Exception {

        log.warn("Starting Penrose.");

        PenroseConfig penroseConfig = new DefaultPenroseConfig();

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        Penrose penrose = penroseFactory.createPenrose(penroseConfig);
        penrose.start();

        log.warn("Creating partition.");

        PartitionConfig partitionConfig = new PartitionConfig("Example");

        EntryConfig rootEntry = createRootEntry();
        partitionConfig.getDirectoryConfig().addEntryConfig(rootEntry);

        EntryConfig usersEntry = createUsersEntry();
        partitionConfig.getDirectoryConfig().addEntryConfig(usersEntry);

        PartitionManager partitionManager = penrose.getPartitionManager();
        partitionManager.createPartition(partitionConfig);

        log.warn("Connecting to Penrose.");

        Session session = penrose.createSession();
        session.bind("uid=admin,ou=system", "secret");

        log.warn("Searching all entries.");

        SearchResponse response = session.search("dc=Example,dc=com", "(objectClass=*)");

        while (response.hasNext()) {
            SearchResult searchResult = response.next();
            log.warn("Entry:\n"+toString(searchResult));
        }

        penrose.stop();
    }

    public String toString(SearchResult searchResult) throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("dn: ");
        sb.append(searchResult.getDn());
        sb.append("\n");

        Attributes attributes = searchResult.getAttributes();
        for (Attribute attribute : attributes.getAll()) {

            String name = attribute.getName();
            Collection values = attribute.getValues();

            for (Object value : values) {
                sb.append(name);
                sb.append(": ");
                sb.append(value);
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    public static void main(String args[]) throws Exception {
        Demo demo = new Demo();
        demo.run();
    }
}
