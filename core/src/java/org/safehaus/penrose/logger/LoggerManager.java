package org.safehaus.penrose.logger;

import org.apache.log4j.Logger;
import org.safehaus.penrose.logger.log4j.Log4jConfigReader;
import org.safehaus.penrose.logger.log4j.Log4jConfig;

import java.util.*;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class LoggerManager {

    Logger log = Logger.getLogger(LoggerManager.class);

    Map loggers = new TreeMap();

    public void load(File dir) throws Exception {
        Log4jConfigReader reader = new Log4jConfigReader(new File(dir, "conf/log4j.xml"));
        Log4jConfig loggingConfig = reader.read();
    }

    public void addLogger(String name) {
        StringTokenizer st = new StringTokenizer(name, ".");
        Map map = loggers;

        //log.debug("Adding logger:");
        while (st.hasMoreTokens()) {
            String rname = st.nextToken();
            //log.debug(" - "+rname);

            Map m = (Map)map.get(rname);
            if (m == null) {
                m = new TreeMap();
                map.put(rname, m);
            }

            map = m;
        }
    }

    public void removeLogger(String name) {
        StringTokenizer st = new StringTokenizer(name, ".");
        Map map = loggers;

        //log.debug("Adding logger:");
        while (st.hasMoreTokens()) {
            String rname = st.nextToken();
            //log.debug(" - "+rname);

            Map m = (Map)map.get(rname);
            if (m == null) {
                m = new TreeMap();
                map.put(rname, m);
            }

            if (!st.hasMoreTokens()) {
                map.remove(rname);
                return;
            }
            
            map = m;
        }
    }

    public Collection getLoggers() {
        return getLoggers(null);
    }

    public Collection getLoggers(String name) {
        if (name == null) {
            return loggers.keySet();
        }

        StringTokenizer st = new StringTokenizer(name, ".");
        Map map = loggers;

        //log.debug("Getting logger:");
        while (st.hasMoreTokens()) {
            String rname = st.nextToken();
            //log.debug(" - "+rname);

            Map m = (Map)map.get(rname);
            if (m == null) {
                m = new TreeMap();
                map.put(rname, m);
            }

            map = m;
        }

        Collection list = new ArrayList();
        for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
            String rname = (String)i.next();
            list.add(name+"."+rname);
        }

        return list;
    }

    public void clear() {
        loggers.clear();
    }
}
