package org.safehaus.penrose.partition;

import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionContext implements Cloneable {

    private String path;

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    public PartitionContext() {
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
    }

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
