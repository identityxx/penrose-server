package org.safehaus.penrose.service;

import org.safehaus.penrose.server.PenroseServer;

/**
 * @author Endi Sukma Dewata
 */
public class ServiceContext implements Cloneable {
    
    protected String path;

    protected PenroseServer penroseServer;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
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
}
