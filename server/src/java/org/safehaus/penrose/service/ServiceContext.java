package org.safehaus.penrose.service;

import org.safehaus.penrose.server.PenroseServer;

import java.io.File;

/**
 * @author Endi Sukma Dewata
 */
public class ServiceContext implements Cloneable {
    
    protected File path;

    protected PenroseServer penroseServer;

    private ClassLoader classLoader;

    public File getPath() {
        return path;
    }

    public void setPath(File path) {
        this.path = path;
    }

    public PenroseServer getPenroseServer() {
        return penroseServer;
    }

    public void setPenroseServer(PenroseServer penroseServer) {
        this.penroseServer = penroseServer;
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
