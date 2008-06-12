package org.safehaus.penrose.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.ArrayList;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class MonitorConfigManager {

    public Logger log = LoggerFactory.getLogger(getClass());

    public MonitorReader monitorReader = new MonitorReader();

    private Map<String,MonitorConfig> monitorConfigs = new LinkedHashMap<String,MonitorConfig>();
    private File monitorsDir;

    public MonitorConfigManager(File monitorsDir) throws Exception {
        this.monitorsDir = monitorsDir;
    }

    public Collection<String> getAvailableMonitorNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        for (File monitorDir : monitorsDir.listFiles()) {
            list.add(monitorDir.getName());
        }
        return list;
    }

    public MonitorConfig load(String monitorName) throws Exception {

        File dir = new File(monitorsDir, monitorName);
        log.debug("Loading monitor from "+dir+".");

        return monitorReader.read(dir);
    }

    public void addMonitorConfig(MonitorConfig monitorConfig) {
        monitorConfigs.put(monitorConfig.getName(), monitorConfig);
    }

    public MonitorConfig getMonitorConfig(String name) {
        return monitorConfigs.get(name);
    }

    public Collection<String> getMonitorNames() {
        return monitorConfigs.keySet();
    }

    public Collection<MonitorConfig> getMonitorConfigs() {
        return monitorConfigs.values();
    }

    public MonitorConfig removeMonitorConfig(String name) {
        return monitorConfigs.remove(name);
    }

    public void clear() {
        monitorConfigs.clear();
    }

    public File getMonitorsDir() {
        return monitorsDir;
    }

    public void setMonitorsDir(File monitorsDir) {
        this.monitorsDir = monitorsDir;
    }
}