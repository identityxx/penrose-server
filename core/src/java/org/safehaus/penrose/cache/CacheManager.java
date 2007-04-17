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
import org.safehaus.penrose.source.SourceSyncManager;
import org.safehaus.penrose.source.SourceSync;
import org.apache.log4j.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.File;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

/**
 * @author Endi S. Dewata
 */
public class CacheManager {

    public Logger log = Logger.getLogger(getClass());

    PartitionManager partitionManager;
    SourceSyncManager sourceSyncManager;

    public CacheManager(
            PartitionManager partitionManager,
            SourceSyncManager sourceSyncManager
    ) throws Exception {

        this.partitionManager = partitionManager;
        this.sourceSyncManager = sourceSyncManager;
    }

    public void status() throws Exception {
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            status(partition);
        }
    }

    public void status(Partition partition) throws Exception {
        Collection caches = sourceSyncManager.getSourceSyncs(partition);
        for (Iterator j=caches.iterator(); j.hasNext(); ) {
            SourceSync sourceSync = (SourceSync)j.next();
            status(sourceSync);
        }
    }

    public void status(SourceSync sourceSync) throws Exception {
        try {
            log.warn("Cache status for "+sourceSync.getPartition().getName()+"/"+sourceSync.getName()+".");
            sourceSync.status();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void create() throws Exception {
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            create(partition);
        }
    }

    public void create(Partition partition) throws Exception {
        Collection caches = sourceSyncManager.getSourceSyncs(partition);
        for (Iterator j=caches.iterator(); j.hasNext(); ) {
            SourceSync sourceSync = (SourceSync)j.next();
            create(sourceSync);
        }
    }

    public void create(SourceSync sourceSync) throws Exception {
        try {
            log.warn("Creating cache tables for "+sourceSync.getPartition().getName()+"/"+sourceSync.getName()+".");
            sourceSync.create();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void load() throws Exception {
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            load(partition);
        }
    }

    public void load(Partition partition) throws Exception {
        Collection caches = sourceSyncManager.getSourceSyncs(partition);
        for (Iterator j=caches.iterator(); j.hasNext(); ) {
            SourceSync sourceSync = (SourceSync)j.next();
            load(sourceSync);
        }
    }

    public void load(SourceSync sourceSync) throws Exception {
        try {
            log.warn("Loading cache data for "+sourceSync.getPartition().getName()+"/"+sourceSync.getName()+".");
            sourceSync.load();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void sync() throws Exception {
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            sync(partition);
        }
    }

    public void sync(Partition partition) throws Exception {
        Collection caches = sourceSyncManager.getSourceSyncs(partition);
        for (Iterator j=caches.iterator(); j.hasNext(); ) {
            SourceSync sourceSync = (SourceSync)j.next();
            sync(sourceSync);
        }
    }

    public void sync(SourceSync sourceSync) throws Exception {
        try {
            log.warn("Synchronizing cache data for "+sourceSync.getPartition().getName()+"/"+sourceSync.getName()+".");
            sourceSync.synchronize();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void clean() throws Exception {
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            clean(partition);
        }
    }

    public void clean(Partition partition) throws Exception {
        Collection caches = sourceSyncManager.getSourceSyncs(partition);
        for (Iterator j=caches.iterator(); j.hasNext(); ) {
            SourceSync sourceSync = (SourceSync)j.next();
            clean(sourceSync);
        }
    }

    public void clean(SourceSync sourceSync) throws Exception {
        try {
            log.warn("Cleaning cache tables for "+sourceSync.getPartition().getName()+"/"+sourceSync.getName()+".");
            sourceSync.clean();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void drop() throws Exception {
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            drop(partition);
        }
    }

    public void drop(Partition partition) throws Exception {
        Collection caches = sourceSyncManager.getSourceSyncs(partition);
        for (Iterator j=caches.iterator(); j.hasNext(); ) {
            SourceSync sourceSync = (SourceSync)j.next();
            drop(sourceSync);
        }
    }

    public void drop(SourceSync sourceSync) throws Exception {
        try {
            log.warn("Dropping cache tables for "+sourceSync.getPartition().getName()+"/"+sourceSync.getName()+".");
            sourceSync.drop();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.cache.CacheManager [OPTION]... <COMMAND>");
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

        Getopt getopt = new Getopt("CacheManager", args, "-:?dv", longopts);

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

        String home = System.getProperty("penrose.home");

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        File log4jProperties = new File((home == null ? "" : home+File.separator)+"conf"+File.separator+"log4j.properties");

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

        PenroseConfig penroseConfig = new PenroseConfig();
        penroseConfig.setHome(home);

        PenroseConfigReader reader = new PenroseConfigReader((home == null ? "" : home+File.separator)+"conf"+File.separator+"server.xml");
        reader.read(penroseConfig);

        PenroseContext penroseContext = new PenroseContext();
        penroseContext.init(penroseConfig);
        penroseContext.load();
        penroseContext.start();

        PartitionManager partitionManager = penroseContext.getPartitionManager();
        SourceSyncManager sourceSyncManager = penroseContext.getSourceSyncManager();

        CacheManager cacheManager = new CacheManager(partitionManager, sourceSyncManager);

        Partition partition = null;
        SourceSync sourceSync = null;

        Iterator iterator = parameters.iterator();
        String command = (String)iterator.next();

        if (iterator.hasNext()) {
            String partitionName = (String)iterator.next();
            partition = partitionManager.getPartition(partitionName);
            if (partition == null) throw new Exception("Partition "+partitionName+" not found.");

            if (iterator.hasNext()) {
                String sourceName = (String)iterator.next();
                sourceSync = sourceSyncManager.getSourceSync(partition, sourceName);
                if (sourceSync == null) throw new Exception("Source sync "+sourceName+" not found.");
            }
        }

        if (sourceSync != null) {

            if ("create".equals(command)) {
                cacheManager.create(sourceSync);

            } else if ("load".equals(command)) {
                cacheManager.load(sourceSync);

            } else if ("sync".equals(command)) {
                cacheManager.sync(sourceSync);

            } else if ("clean".equals(command)) {
                cacheManager.clean(sourceSync);

            } else if ("drop".equals(command)) {
                cacheManager.drop(sourceSync);

            } else if ("status".equals(command)) {
                cacheManager.status(sourceSync);
            }

        } else if (partition != null) {

            if ("create".equals(command)) {
                cacheManager.create(partition);

            } else if ("load".equals(command)) {
                cacheManager.load(partition);

            } else if ("sync".equals(command)) {
                cacheManager.sync(partition);

            } else if ("clean".equals(command)) {
                cacheManager.clean(partition);

            } else if ("drop".equals(command)) {
                cacheManager.drop(partition);

            } else if ("status".equals(command)) {
                cacheManager.status(partition);
            }

        } else {
            if ("create".equals(command)) {
                cacheManager.create();

            } else if ("load".equals(command)) {
                cacheManager.load();

            } else if ("sync".equals(command)) {
                cacheManager.sync();

            } else if ("clean".equals(command)) {
                cacheManager.clean();

            } else if ("drop".equals(command)) {
                cacheManager.drop();

            } else if ("status".equals(command)) {
                cacheManager.status();
            }
        }

        penroseContext.stop();

        System.exit(0);
    }

}
