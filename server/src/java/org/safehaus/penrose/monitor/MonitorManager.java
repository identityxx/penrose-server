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
package org.safehaus.penrose.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Endi S. Dewata
 */
public class MonitorManager {

    public Logger log = LoggerFactory.getLogger(getClass());

    // http://lopica.sourceforge.net/os.html
    public final static String Linux   = "Linux";
    public final static String Windows = "Windows";
    public final static String MacOSX  = "Mac OS X";
    public final static String Solaris = "Solaris";
    public final static String SunOS   = "SunOS";

    private File home;

    private Map<String,Monitor> monitors = new LinkedHashMap<String,Monitor>();
    private MonitorConfigManager monitorConfigManager;

    public String osName;

    public String linuxServiceName   = "penrose";

    public String windowsServiceName;
    public String windowsNet         = "c:\\Windows\\system32\\net";

    boolean locked;

    public MonitorManager(MonitorConfigManager monitorConfigManager) {
        this.monitorConfigManager = monitorConfigManager;
    }

    public void init() throws Exception {
        File buildPropertiesFile = new File(home, "build.properties");

        Properties buildProperties = new Properties();
        buildProperties.load(new FileInputStream(buildPropertiesFile));

        String title = buildProperties.getProperty("project.title");
        String version = buildProperties.getProperty("product.version");

        windowsServiceName = title+" Server "+version;

        Properties systemProperties = System.getProperties();
        //systemProperties.list(System.out);

        osName = systemProperties.getProperty("os.name");

        if (osName.startsWith(Windows+" ")) {
            osName = Windows;

        } else if (osName.equals(Solaris)) {
            osName = SunOS;
        }
    }

    public void addMonitorConfig(MonitorConfig monitorConfig) {
        monitorConfigManager.addMonitorConfig(monitorConfig);
    }

    public File getMonitorsDir() {
        return monitorConfigManager.getMonitorsDir();
    }

    public void addMonitor(Monitor monitor) {
        monitors.put(monitor.getName(), monitor);
    }

    public Monitor getMonitor(String name) {
        return monitors.get(name);
    }

    public Collection<String> getMonitorNames() {
        return monitors.keySet();
    }

    public Collection<Monitor> getMonitors() {
        return monitors.values();
    }

    public Monitor removeMonitor(String name) {
        return monitors.remove(name);
    }

    public void clear() {
        monitors.clear();
    }

    public void loadMonitorConfig(String name) throws Exception {

        log.debug("Loading "+name+" monitor.");

        MonitorConfig monitorConfig = monitorConfigManager.load(name);
        monitorConfigManager.addMonitorConfig(monitorConfig);
    }

    public Monitor startMonitor(String name) throws Exception {

        MonitorConfig monitorConfig = monitorConfigManager.getMonitorConfig(name);

        if (!monitorConfig.isEnabled()) {
            log.debug(monitorConfig.getName()+" monitor is disabled.");
            return null;
        }

        log.debug("Starting "+name+" monitor.");

        File monitorDir = new File(monitorConfigManager.getMonitorsDir(), name);

        Collection<URL> classPaths = monitorConfig.getClassPaths();
        URLClassLoader classLoader = new URLClassLoader(classPaths.toArray(new URL[classPaths.size()]), getClass().getClassLoader());

        Class clazz = classLoader.loadClass(monitorConfig.getMonitorClass());

        Monitor monitor = (Monitor)clazz.newInstance();

        MonitorContext monitorContext = new MonitorContext();
        monitorContext.setPath(monitorDir);
        monitorContext.setMonitorManager(this);
        monitorContext.setClassLoader(classLoader);

        monitor.init(monitorConfig, monitorContext);

        addMonitor(monitor);

        monitor.start();

        return monitor;
    }

    public void stopMonitor(String name) throws Exception {

        log.debug("Stopping "+name+" monitor.");

        Monitor monitor = getMonitor(name);
        if (monitor == null) return;

        monitor.stop();

        monitors.remove(name);
    }

    public void unloadMonitor(String name) throws Exception {
        monitorConfigManager.removeMonitorConfig(name);
    }

    public String getMonitorStatus(String name) {

        Monitor monitor = monitors.get(name);

        if (monitor == null) {
            return "STOPPED";
        } else {
            return "STARTED";
        }
    }

    public MonitorConfigManager getMonitorConfigManager() {
        return monitorConfigManager;
    }

    public void setMonitorConfigManager(MonitorConfigManager monitorConfigManager) {
        this.monitorConfigManager = monitorConfigManager;
    }

    public MonitorConfig getMonitorConfig(String monitorName) {
        return monitorConfigManager.getMonitorConfig(monitorName);
    }

    public File getHome() {
        return home;
    }

    public void setHome(File home) {
        this.home = home;
    }

    public boolean isWindows() throws Exception {
        return osName.equals(Windows);
    }

    public boolean isLinux() throws Exception {
        return osName.equals(Linux);
    }

    public boolean isMacOSX() throws Exception {
        return osName.equals(MacOSX);
    }

    public boolean isSunOS() throws Exception {
        return osName.equals(SunOS);
    }

    public synchronized boolean lock() {
        if (locked) return false;
        locked = true;
        return true;
    }

    public synchronized void unlock() {
        locked = false;
    }

    public void restart() {

        if (!lock()) {
            log.warn("Penrose Server is being restarted.");
            return;
        }

        try {
            log.warn("Restarting Penrose Server.");

            try {
                stop();
            } catch (Exception e) {
                log.warn(e.getMessage());
            }
            start();

            log.warn("Penrose Server has been restarted.");

        } catch (Exception e) {
            log.error("Penrose Server can't be restarted.");
            log.error(e.getMessage(), e);

        } finally {
            unlock();
        }
    }

    public void start() throws Exception {
        if (isLinux()) {
            startLinux();

        } else if (isWindows()) {
            startWindows();

        } else {
            startUnix();
        }
    }

    public void stop() throws Exception {
        if (isLinux()) {
            stopLinux();

        } else if (isWindows()) {
            stopWindows();

        } else {
            stopUnix();
        }
    }

    public void startLinux() throws Exception {
        execute("service "+linuxServiceName+" start");
    }

    public void stopLinux() throws Exception {
        execute("service "+linuxServiceName+" stop");
    }

    public void startWindows() throws Exception {
        execute(windowsNet+" start \""+ windowsServiceName +"\"");
    }

    public void stopWindows() throws Exception {
        execute(windowsNet+" stop \""+ windowsServiceName +"\"");
    }

    public void startUnix() throws Exception {
        execute(home+"/bin/vd-server.sh start");
    }

    public void stopUnix() throws Exception {
        execute(home+"/bin/vd-server.sh stop");
    }

    public void execute(String command) throws Exception {
        log.debug("Executing "+command);

        Runtime runtime = Runtime.getRuntime();

        Process p = runtime.exec(command);
        int rc = p.waitFor();

        if (rc != 0) throw new Exception("Command failed to execute. RC: "+rc);
    }
}