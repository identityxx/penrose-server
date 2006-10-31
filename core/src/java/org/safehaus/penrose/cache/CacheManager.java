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

import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connector.JDBCAdapter;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.partition.*;
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

    public static Logger log = Logger.getLogger(CacheManager.class);

    public CacheManager() throws Exception {
    }

    public static void create(Penrose penrose) throws Exception {
        Connector connector = penrose.getConnector();
        SourceCacheManager sourceCacheManager = connector.getSourceCacheManager();
        sourceCacheManager.create();

        Handler handler = penrose.getHandler();
        EntryCache entryCache = handler.getEntryCache();

        PartitionManager partitionManager = penrose.getPartitionManager();

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();

            entryCache.create(partition);
        }
    }

    public static void load(Penrose penrose) throws Exception {
        Connector connector = penrose.getConnector();
        SourceCacheManager sourceCacheManager = connector.getSourceCacheManager();
        sourceCacheManager.load();

        Handler handler = penrose.getHandler();
        EntryCache entryCache = handler.getEntryCache();

        PartitionManager partitionManager = penrose.getPartitionManager();

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            entryCache.load(penrose, partition);
        }
    }

    public static void clean(Penrose penrose) throws Exception {

        Handler handler = penrose.getHandler();
        EntryCache entryCache = handler.getEntryCache();

        PartitionManager partitionManager = penrose.getPartitionManager();

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            entryCache.clean(partition);
        }

        Connector connector = penrose.getConnector();
        SourceCacheManager sourceCacheManager = connector.getSourceCacheManager();
        sourceCacheManager.clean();
    }

    public static void drop(Penrose penrose) throws Exception {

        Handler handler = penrose.getHandler();
        EntryCache entryCache = handler.getEntryCache();

        PartitionManager partitionManager = penrose.getPartitionManager();

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            entryCache.drop(partition);
        }

        Connector connector = penrose.getConnector();
        SourceCacheManager sourceCacheManager = connector.getSourceCacheManager();
        sourceCacheManager.drop();
    }

    public static void changeTable(Penrose penrose) throws Exception {

        PartitionManager partitionManager = penrose.getPartitionManager();

        Collection partitions = partitionManager.getPartitions();
        for (Iterator i=partitions.iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();

            Collection sourceConfigs = partition.getSourceConfigs();
            for (Iterator j=sourceConfigs.iterator(); j.hasNext(); ) {
                SourceConfig sourceConfig = (SourceConfig)j.next();

                String connectionName = sourceConfig.getConnectionName();
                ConnectionConfig connectionConfig = partition.getConnectionConfig(connectionName);

                if (!"JDBC".equals(connectionConfig.getAdapterName())) continue;

                String catalog = sourceConfig.getParameter(JDBCAdapter.CATALOG);
                String schema = sourceConfig.getParameter(JDBCAdapter.SCHEMA);
                String tableName = sourceConfig.getParameter(JDBCAdapter.TABLE);
                if (tableName == null) tableName = sourceConfig.getParameter(JDBCAdapter.TABLE_NAME);
                if (catalog != null) tableName = catalog +"."+tableName;
                if (schema != null) tableName = schema +"."+tableName;

                Collection primaryKeyFieldConfigs = sourceConfig.getPrimaryKeyFieldConfigs();

                generateCreateTable(tableName, primaryKeyFieldConfigs);
                generateAddTrigger(tableName, primaryKeyFieldConfigs);
                generateModifyTrigger(tableName, primaryKeyFieldConfigs);
                generateDeleteTrigger(tableName, primaryKeyFieldConfigs);
            }
        }
    }

    public static void generateCreateTable(String tableName, Collection primaryKeyFieldConfigs) throws Exception {
        System.out.println("create table "+tableName+"_changes (");
        System.out.println("    changeNumber integer auto_increment,");
        System.out.println("    changeTime datetime,");
        System.out.println("    changeAction varchar(10),");
        System.out.println("    changeUser varchar(10),");

        for (Iterator i=primaryKeyFieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            System.out.println("    "+fieldConfig.getName()+" "+fieldConfig.getType()+",");
        }

        System.out.println("    primary key (changeNumber)");
        System.out.println(");");
    }

    public static void generateAddTrigger(String tableName, Collection primaryKeyFieldConfigs) throws Exception {
        System.out.println("create trigger "+tableName+"_add after insert on "+tableName);
        System.out.println("for each row insert into "+tableName+"_changes values (");
        System.out.println("    null,");
        System.out.println("    now(),");
        System.out.println("    'ADD',");
        System.out.println("    substring_index(user(),_utf8'@',1),");

        for (Iterator i=primaryKeyFieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            System.out.print("    new."+fieldConfig.getName());
            if (i.hasNext()) System.out.print(",");
            System.out.println();
        }

        System.out.println(");");
    }

    public static void generateModifyTrigger(String tableName, Collection primaryKeyFieldConfigs) throws Exception {
        System.out.println("delimiter |");
        System.out.println("create trigger "+tableName+"_modify after update on "+tableName);
        System.out.println("for each row begin");

        System.out.print("    if ");
        for (Iterator i=primaryKeyFieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            System.out.print("new."+fieldConfig.getName()+" = old."+fieldConfig.getName());
            if (i.hasNext()) System.out.print(" and ");
        }
        System.out.println(" then");

        System.out.println("        insert into "+tableName+"_changes values (");
        System.out.println("            null,");
        System.out.println("            now(),");
        System.out.println("            'MODIFY',");
        System.out.println("            substring_index(user(),_utf8'@',1),");

        for (Iterator i=primaryKeyFieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            System.out.print("            new."+fieldConfig.getName());
            if (i.hasNext()) System.out.print(",");
            System.out.println();
        }

        System.out.println("        );");
        System.out.println("    else");
        System.out.println("        insert into "+tableName+"_changes values (");
        System.out.println("            null,");
        System.out.println("            now(),");
        System.out.println("            'DELETE',");
        System.out.println("            substring_index(user(),_utf8'@',1),");

        for (Iterator i=primaryKeyFieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            System.out.print("            old."+fieldConfig.getName());
            if (i.hasNext()) System.out.print(",");
            System.out.println();
        }

        System.out.println("        );");
        System.out.println("        insert into "+tableName+"_changes values (");
        System.out.println("            null,");
        System.out.println("            now(),");
        System.out.println("            'ADD',");
        System.out.println("            substring_index(user(),_utf8'@',1),");

        for (Iterator i=primaryKeyFieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            System.out.print("            new."+fieldConfig.getName());
            if (i.hasNext()) System.out.print(",");
            System.out.println();
        }

        System.out.println("        );");
        System.out.println("    end if;");
        System.out.println("end;|");
        System.out.println("delimiter ;");
    }

    public static void generateDeleteTrigger(String tableName, Collection primaryKeyFieldConfigs) throws Exception {
        System.out.println("create trigger "+tableName+"_delete after delete on "+tableName);
        System.out.println("for each row insert into "+tableName+"_changes values (");
        System.out.println("    null,");
        System.out.println("    now(),");
        System.out.println("    'DELETE',");
        System.out.println("    substring_index(user(),_utf8'@',1),");

        for (Iterator i=primaryKeyFieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            System.out.print("    old."+fieldConfig.getName());
            if (i.hasNext()) System.out.print(",");
            System.out.println();
        }

        System.out.println(");");
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.cache.CacheManager [OPTION]... <COMMAND>");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  create             create cache tables");
        System.out.println("  load               load data into cache tables");
        System.out.println("  clean              clean data from cache tables");
        System.out.println("  drop               drop cache tables");
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

        Collection parameters = new ArrayList();
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

        String homeDirectory = System.getProperty("penrose.home");

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        File log4jProperties = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"log4j.properties");

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

        Iterator iterator = parameters.iterator();
        String command = (String)iterator.next();

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        Penrose penrose = penroseFactory.createPenrose(homeDirectory);
        penrose.start();

        if ("create".equals(command)) {
            create(penrose);

        } else if ("load".equals(command)) {
            load(penrose);

        } else if ("clean".equals(command)) {
            clean(penrose);

        } else if ("drop".equals(command)) {
            drop(penrose);

        } else if ("changeTable".equals(command)) {
            changeTable(penrose);
        }

        penrose.stop();

        System.exit(0);
    }

}
