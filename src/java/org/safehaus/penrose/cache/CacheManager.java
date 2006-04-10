/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.engine.Engine;
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

        String logLevel = "NORMAL";

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
                    logLevel = "DEBUG";
                    break;
                case 'v':
                    logLevel = "VERBOSE";
                    break;
            }
        }

        if (parameters.size() == 0) {
            showUsage();
            System.exit(0);
        }

        String homeDirectory = System.getProperty("penrose.home");

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.toLevel("OFF"));

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        File log4jProperties = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"log4j.properties");

        if (log4jProperties.exists()) {
            PropertyConfigurator.configure(log4jProperties.getAbsolutePath());

        } else if (logLevel.equals("DEBUG")) {
            logger.setLevel(Level.toLevel("DEBUG"));
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("%-20C{1} [%4L] %m%n"));
            BasicConfigurator.configure(appender);

        } else if (logLevel.equals("VERBOSE")) {
            logger.setLevel(Level.toLevel("INFO"));
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
            BasicConfigurator.configure(appender);

        } else {
            logger.setLevel(Level.toLevel("WARN"));
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
        }

        penrose.stop();
    }

    public static void create(Penrose penrose) throws Exception {
        Connector connector = penrose.getConnector();
        SourceCache sourceCache = connector.getSourceCache();
        sourceCache.create();

        Engine engine = penrose.getEngine();
        EntryCache entryCache = engine.getEntryCache();
        entryCache.create();
    }

    public static void load(Penrose penrose) throws Exception {
        Connector connector = penrose.getConnector();
        SourceCache sourceCache = connector.getSourceCache();
        sourceCache.load();

        Engine engine = penrose.getEngine();
        EntryCache entryCache = engine.getEntryCache();
        entryCache.load(penrose);
    }

    public static void clean(Penrose penrose) throws Exception {

        Engine engine = penrose.getEngine();
        EntryCache entryCache = engine.getEntryCache();
        entryCache.clean();

        Connector connector = penrose.getConnector();
        SourceCache sourceCache = connector.getSourceCache();
        sourceCache.clean();
    }

    public static void drop(Penrose penrose) throws Exception {

        Engine engine = penrose.getEngine();
        EntryCache entryCache = engine.getEntryCache();
        entryCache.drop();

        Connector connector = penrose.getConnector();
        SourceCache sourceCache = connector.getSourceCache();
        sourceCache.drop();
    }

}
