package org.safehaus.penrose.jdbc;

/**
 * @author Endi S. Dewata
 */
public class UpdateResponse extends Response {

    protected int rowCount;

    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }
}
