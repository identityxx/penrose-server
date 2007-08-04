package org.safehaus.penrose.partition;

import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;

import java.io.File;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionContext implements Cloneable {

    private File path;

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    public PartitionContext() {
    }

    public File getPath() {
        return path;
    }

    public void setPath(File path) {
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
