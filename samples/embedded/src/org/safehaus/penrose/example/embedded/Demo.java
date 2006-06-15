package org.safehaus.penrose.example.embedded;

import org.apache.log4j.*;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;

import javax.naming.directory.SearchResult;
import javax.naming.directory.Attributes;
import javax.naming.directory.Attribute;
import javax.naming.NamingEnumeration;
import java.util.Iterator;

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

    public EntryMapping createRootEntry() {

        EntryMapping entryMapping = new EntryMapping();
        entryMapping.setDn("dc=Example,dc=com");

        entryMapping.addObjectClass("dcObject");
        entryMapping.addObjectClass("organization");

        AttributeMapping attribute = new AttributeMapping();
        attribute.setName("dc");
        attribute.setType(AttributeMapping.CONSTANT);
        attribute.setConstant("Example");
        attribute.setRdn(true);

        entryMapping.addAttributeMapping(attribute);

        attribute = new AttributeMapping();
        attribute.setName("o");
        attribute.setType(AttributeMapping.CONSTANT);
        attribute.setConstant("Example");

        entryMapping.addAttributeMapping(attribute);

        return entryMapping;
    }

    public EntryMapping createUsersEntry() {

        EntryMapping entryMapping = new EntryMapping();
        entryMapping.setDn("ou=Users,dc=Example,dc=com");

        entryMapping.addObjectClass("organizationalUnit");

        AttributeMapping attribute = new AttributeMapping();
        attribute.setName("ou");
        attribute.setType(AttributeMapping.CONSTANT);
        attribute.setConstant("Users");
        attribute.setRdn(true);

        entryMapping.addAttributeMapping(attribute);

        return entryMapping;
    }

    public void run() throws Exception {

        log.warn("Creating partition.");

        PartitionConfig partitionConfig = new PartitionConfig();
        partitionConfig.setName("Example");

        Partition partition = new Partition(partitionConfig);

        EntryMapping rootEntry = createRootEntry();
        partition.addEntryMapping(rootEntry);

        EntryMapping usersEntry = createUsersEntry();
        partition.addEntryMapping(usersEntry);

        log.warn("Configuring Penrose.");

        PenroseConfig penroseConfig = new DefaultPenroseConfig();
        penroseConfig.removePartitionConfig("DEFAULT");

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        Penrose penrose = penroseFactory.createPenrose(penroseConfig);

        PartitionManager partitionManager = penrose.getPartitionManager();
        partitionManager.addPartition(partition);

        log.warn("Starting Penrose.");

        penrose.start();

        log.warn("Connecting to Penrose.");

        PenroseSession session = penrose.newSession();
        session.bind("uid=admin,ou=system", "secret");

        log.warn("Searching all entries.");

        PenroseSearchResults results = new PenroseSearchResults();
        PenroseSearchControls sc = new PenroseSearchControls();

        session.search(
                "dc=Example,dc=com",
                "(objectClass=*)",
                sc,
                results);

        for (Iterator i = results.iterator(); i.hasNext();) {
            SearchResult entry = (SearchResult)i.next();
            log.warn("Entry:\n"+toString(entry));
        }

        penrose.stop();
    }

    public String toString(SearchResult entry) throws Exception {

        StringBuffer sb = new StringBuffer();
        sb.append("dn: "+entry.getName()+"\n");

        Attributes attributes = entry.getAttributes();
        for (NamingEnumeration i=attributes.getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            String name = attribute.getID();

            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();
                sb.append(name+": "+value+"\n");
            }
        }

        return sb.toString();
    }

    public static void main(String args[]) throws Exception {
        Demo demo = new Demo();
        demo.run();
    }
}
