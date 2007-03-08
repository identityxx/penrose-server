package org.safehaus.penrose.connector;

import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.partition.SourceConfig;

/**
 * @author Endi S. Dewata
 */
public class ConnectorSearchResult {

    public AttributeValues sourceValues;

    public EntryMapping entryMapping;
    public SourceMapping sourceMapping;
    public SourceConfig sourceConfig;

    public ConnectorSearchResult(AttributeValues sourceValues) {
        this.sourceValues = sourceValues;
    }

    public SourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public void setSourceConfig(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    public AttributeValues getSourceValues() {
        return sourceValues;
    }

    public void setSourceValues(AttributeValues sourceValues) {
        this.sourceValues = sourceValues;
    }

    public EntryMapping getEntryMapping() {
        return entryMapping;
    }

    public void setEntryMapping(EntryMapping entryMapping) {
        this.entryMapping = entryMapping;
    }

    public SourceMapping getSourceMapping() {
        return sourceMapping;
    }

    public void setSourceMapping(SourceMapping sourceMapping) {
        this.sourceMapping = sourceMapping;
    }
}
