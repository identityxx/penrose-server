package org.safehaus.penrose.source;

import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.partition.SourceConfig;

/**
 * @author Endi S. Dewata
 */
public class Source {
    
    private SourceMapping sourceMapping;
    private SourceConfig sourceConfig;

    public SourceMapping getSourceMapping() {
        return sourceMapping;
    }

    public void setSourceMapping(SourceMapping sourceMapping) {
        this.sourceMapping = sourceMapping;
    }

    public SourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public void setSourceConfig(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }
}
