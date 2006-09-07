package org.safehaus.penrose.performance;

import junit.framework.TestCase;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.partition.PartitionConfig;
import org.apache.log4j.*;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Date;
import java.text.NumberFormat;
import java.text.DecimalFormat;

/**
 * @author Endi S. Dewata
 */
public class SearchEmbeddedTest extends TestCase {

    PenroseConfig penroseConfig;
    Penrose penrose;

    public void setUp() throws Exception {

        ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
        BasicConfigurator.configure(appender);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        logger.setLevel(Level.WARN);

        penroseConfig = new DefaultPenroseConfig();

        PartitionConfig partitionConfig = new PartitionConfig("jdbc", "samples/jdbc/partition");
        penroseConfig.addPartitionConfig(partitionConfig);

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(penroseConfig);
        penrose.start();

    }

    public void tearDown() throws Exception {
        penrose.stop();
    }

    public void testSearchRate() throws Exception {
        final Collection list = new ArrayList();
        int count = 1000;

        PenroseSession session = penrose.newSession();

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_BASE);

        list.add(new Date());

        for (int i=0; i<count; i++) {
            System.out.println("Search "+(i+1)+" of "+count);

            PenroseSearchResults results = new PenroseSearchResults();

            session.search("uid=jstockton,ou=Users,dc=JDBC,dc=Example,dc=com", "(objectClass=*)", sc, results);

            while (results.hasNext()) results.next();

            list.add(new Date());

            int rc = results.getReturnCode();
            assertTrue("Search completed succesfully.", rc == 0);
        }

        session.close();

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
