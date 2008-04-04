/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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
package org.safehaus.penrose.cache;

import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.PenroseConfigReader;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceManager;
import org.safehaus.penrose.scheduler.SchedulerConfig;
import org.safehaus.penrose.scheduler.JobConfig;
import org.safehaus.penrose.jdbc.scheduler.JDBCSyncJob;
import org.apache.log4j.*;

import java.util.*;
import java.io.File;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

/**
 * @author Endi S. Dewata
 */
public class CacheUtil {

    public static Logger log = Logger.getLogger(CacheUtil.class);
    public static boolean debug = log.isDebugEnabled();

    PartitionManager partitionManager;

    public CacheUtil(
            PartitionManager partitionManager
    ) throws Exception {

        this.partitionManager = partitionManager;
    }

    public Collection<Source> getCaches(Partition partition) throws Exception {

        Collection<Source> list = new ArrayList<Source>();

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        SchedulerConfig schedulerConfig = partitionConfig.getSchedulerConfig();
        SourceManager sourceManager = partition.getSourceManager();

        for (JobConfig jobConfig : schedulerConfig.getJobConfigs()) {
            String jobClass = jobConfig.getJobClass();
            if (!jobClass.equals(JDBCSyncJob.class.getName())) continue;

            String target = jobConfig.getParameter("target");
            StringTokenizer st = new StringTokenizer(target, ";, ");
            while (st.hasMoreTokens()) {
                String sourceName = st.nextToken();
                Source s = sourceManager.getSource(sourceName);
                list.add(s);
            }
        }

        return list;
    }

    public Collection<Source> getCaches(Source source) throws Exception {

        Collection<Source> list = new ArrayList<Source>();

        Partition partition = source.getPartition();
        PartitionConfig partitionConfig = partition.getPartitionConfig();
        SchedulerConfig schedulerConfig = partitionConfig.getSchedulerConfig();
        SourceManager sourceManager = partition.getSourceManager();

        JobConfig jobConfig = schedulerConfig.getJobConfig(source.getName());
        String jobClass = jobConfig.getJobClass();
        if (!jobClass.equals(JDBCSyncJob.class.getName())) return list;

        String target = jobConfig.getParameter("target");
        StringTokenizer st = new StringTokenizer(target, ";, ");
        while (st.hasMoreTokens()) {
            String sourceName = st.nextToken();
            Source s = sourceManager.getSource(sourceName);
            list.add(s);
        }

        return list;
    }

    public void status() throws Exception {
        for (Partition partition : partitionManager.getPartitions()) {
            status(partition);
        }
    }

    public void status(Partition partition) throws Exception {
        for (Source cache : getCaches(partition)) {
            try {
                cache.status();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void status(Source source) throws Exception {
        for (Source cache : getCaches(source)) {
            try {
                cache.status();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void create() throws Exception {
        for (Partition partition : partitionManager.getPartitions()) {
            create(partition);
        }
    }

    public void create(Partition partition) throws Exception {
        for (Source cache : getCaches(partition)) {
            try {
                cache.create();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void create(Source source) throws Exception {
        for (Source cache : getCaches(source)) {
            try {
                cache.create();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void update() throws Exception {
        for (Partition partition : partitionManager.getPartitions()) {
            update(partition);
        }
    }

    public void update(Partition partition) throws Exception {
    }

    public void update(Source source) throws Exception {
        try {
            log.warn("Cleaning cache tables for "+source.getPartition().getName()+"/"+source.getName()+".");
            source.clear();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void clean() throws Exception {
        for (Partition partition : partitionManager.getPartitions()) {
            clean(partition);
        }
    }

    public void clean(Partition partition) throws Exception {
        for (Source cache : getCaches(partition)) {
            try {
                cache.clear();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void clean(Source source) throws Exception {
        for (Source cache : getCaches(source)) {
            try {
                cache.clear();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void drop() throws Exception {
        for (Partition partition : partitionManager.getPartitions()) {
            drop(partition);
        }
    }

    public void drop(Partition partition) throws Exception {
        for (Source cache : getCaches(partition)) {
            try {
                cache.drop();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void drop(Source source) throws Exception {
        for (Source cache : getCaches(source)) {
            try {
                cache.drop();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public static void showUsage() {
        System.out.println("Usage: "+ CacheUtil.class.getName()+" [OPTION]... <COMMAND>");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  create [partition [source]]   create cache tables");
        System.out.println("  load   [partition [source]]   load data into cache tables");
        System.out.println("  sync   [partition [source]]   sync data in cache tables");
        System.out.println("  clean  [partition [source]]   clean data from cache tables");
        System.out.println("  drop   [partition [source]]   drop cache tables");
        System.out.println("  status [partition [source]]   show cache status");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -?, --help         display this help and exit");
        System.out.println("  -d                 run in debug mode");
        System.out.println("  -v                 run in verbose mode");
    }

    public static void main(String args[]) throws Exception {

        Level logLevel = Level.WARN;

        LongOpt[] longopts = new LongOpt[1];
        longopts[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, '?');

        Getopt getopt = new Getopt("CacheUtil", args, "-:?dv", longopts);

        Collection<String> parameters = new ArrayList<String>();
        int c;
        while ((c = getopt.getopt()) != -1) {
            switch (c) {
                case ':':
                case '?':
                    showUsage();
                    System.exit(0);
                    break;
                case 1:
                    parameters.add(getopt.getOptarg());
                    break;
                case 'd':
                    logLevel = Level.DEBUG;
                    break;
                case 'v':
                    logLevel = Level.INFO;
                    break;
            }
        }

        if (parameters.size() == 0) {
            showUsage();
            System.exit(0);
        }

        File home = new File(System.getProperty("penrose.home"));

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        File log4jProperties = new File(home, "conf"+File.separator+"log4j.properties");

        if (log4jProperties.exists()) {
            PropertyConfigurator.configure(log4jProperties.getAbsolutePath());

        } else if (logLevel.equals(Level.DEBUG)) {
            logger.setLevel(Level.DEBUG);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("%-20C{1} [%4L] %m%n"));
            BasicConfigurator.configure(appender);

        } else if (logLevel.equals(Level.INFO)) {
            logger.setLevel(Level.INFO);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
            BasicConfigurator.configure(appender);

        } else {
            logger.setLevel(Level.WARN);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
            BasicConfigurator.configure(appender);
        }

        File file = new File(home, "conf"+File.separator+"server.xml");

        PenroseConfigReader reader = new PenroseConfigReader();
        PenroseConfig penroseConfig = reader.read(file);

        PenroseContext penroseContext = new PenroseContext(home);
        penroseContext.init(penroseConfig);

        PartitionManager partitionManager = new PartitionManager(
                home,
                penroseConfig,
                penroseContext
        );

        PartitionConfigManager partitionConfigManager = partitionManager.getPartitionConfigManager();

        for (String partitionName : partitionManager.getAvailablePartitionNames()) {

            if (debug) log.debug("----------------------------------------------------------------------------------");

            File dir = new File(partitionManager.getPartitionsDir(), partitionName);

            PartitionConfig partitionConfig = new PartitionConfig(partitionName);
            partitionConfig.load(dir);
            
            partitionConfigManager.addPartitionConfig(partitionConfig);

            String name = partitionConfig.getName();

            if (!partitionConfig.isEnabled()) {
                log.debug(name+" partition is disabled.");
                continue;
            }

            log.debug("Starting "+name+" partition.");

            PartitionFactory partitionFactory = new PartitionFactory();
            partitionFactory.setPartitionsDir(partitionManager.getPartitionsDir());
            partitionFactory.setPenroseConfig(penroseConfig);
            partitionFactory.setPenroseContext(penroseContext);

            Partition partition = partitionFactory.createPartition(partitionConfig);
            partitionManager.addPartition(partition);
        }

        CacheUtil cacheUtil = new CacheUtil(partitionManager);

        Partition partition = null;
        Source source = null;

        Iterator iterator = parameters.iterator();
        String command = (String)iterator.next();
        System.out.println("Command: "+command);

        if (iterator.hasNext()) {
            String partitionName = (String)iterator.next();
            System.out.println("Partition: "+partitionName);
            partition = partitionManager.getPartition(partitionName);

            if (partition == null) throw new Exception("Partition "+partitionName+" not found.");

            if (iterator.hasNext()) {
                String sourceName = (String)iterator.next();
                System.out.println("Source: "+sourceName);

                SourceManager sourceManager = partition.getSourceManager();
                source = sourceManager.getSource(sourceName);
                if (source == null) throw new Exception("Source "+sourceName+" not found.");
            }
        }

        if (source != null) {

            if ("create".equals(command)) {
                cacheUtil.create(source);

            } else if ("clean".equals(command)) {
                cacheUtil.clean(source);

            } else if ("drop".equals(command)) {
                cacheUtil.drop(source);

            } else if ("status".equals(command)) {
                cacheUtil.status(source);
            }

        } else if (partition != null) {

            if ("create".equals(command)) {
                cacheUtil.create(partition);

            } else if ("update".equals(command)) {
                cacheUtil.update(partition);

            } else if ("clean".equals(command)) {
                cacheUtil.clean(partition);

            } else if ("drop".equals(command)) {
                cacheUtil.drop(partition);

            } else if ("status".equals(command)) {
                cacheUtil.status(partition);
            }

        } else {
            if ("create".equals(command)) {
                cacheUtil.create();

            } else if ("update".equals(command)) {
                cacheUtil.update();

            } else if ("clean".equals(command)) {
                cacheUtil.clean();

            } else if ("drop".equals(command)) {
                cacheUtil.drop();

            } else if ("status".equals(command)) {
                cacheUtil.status();
            }
        }

        partitionManager.stopPartitions();
        penroseContext.stop();

        System.exit(0);
    }

}
