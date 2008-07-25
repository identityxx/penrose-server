package org.safehaus.penrose.jdbc;

/**
 * @author Endi Sukma Dewata
 */
public class StatementSource {

    private String alias;
    private String partitionName;
    private String sourceName;

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }
}
