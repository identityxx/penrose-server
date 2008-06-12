package org.safehaus.penrose.monitor;

import java.io.File;

/**
 * @author Endi Sukma Dewata
 */
public class MonitorContext implements Cloneable {

    protected File path;

    protected MonitorManager monitorManager;

    private ClassLoader classLoader;

    public File getHome() {
        return monitorManager.getHome();
    }
    
    public File getPath() {
        return path;
    }

    public void setPath(File path) {
        this.path = path;
    }

    public MonitorManager getMonitorManager() {
        return monitorManager;
    }

    public void setMonitorManager(MonitorManager monitorManager) {
        this.monitorManager = monitorManager;
    }

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }
}