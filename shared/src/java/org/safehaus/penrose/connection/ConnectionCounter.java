package org.safehaus.penrose.connection;

import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class ConnectionCounter implements Serializable {

    private long addCounter;
    private long bindCounter;
    private long deleteCounter;
    private long loadCounter;
    private long modifyCounter;
    private long searchCounter;

    public void reset() {
        addCounter = 0;
        bindCounter = 0;
        deleteCounter = 0;
        loadCounter = 0;
        modifyCounter = 0;
        searchCounter = 0;
    }

    public void incAddCounter() {
        addCounter++;
    }

    public long getAddCounter() {
        return addCounter;
    }

    public void setAddCounter(long addCounter) {
        this.addCounter = addCounter;
    }

    public void incBindCounter() {
        bindCounter++;
    }

    public long getBindCounter() {
        return bindCounter;
    }

    public void setBindCounter(long bindCounter) {
        this.bindCounter = bindCounter;
    }

    public void incDeleteCounter() {
        deleteCounter++;
    }

    public long getDeleteCounter() {
        return deleteCounter;
    }

    public void setDeleteCounter(long deleteCounter) {
        this.deleteCounter = deleteCounter;
    }

    public void incModifyCounter() {
        modifyCounter++;
    }

    public long getModifyCounter() {
        return modifyCounter;
    }

    public void setModifyCounter(long modifyCounter) {
        this.modifyCounter = modifyCounter;
    }

    public void incSearchCounter() {
        searchCounter++;
    }

    public long getSearchCounter() {
        return searchCounter;
    }

    public void setSearchCounter(long searchCounter) {
        this.searchCounter = searchCounter;
    }

    public void incLoadCounter() {
        loadCounter++;
    }

    public long getLoadCounter() {
        return loadCounter;
    }

    public void setLoadCounter(long loadCounter) {
        this.loadCounter = loadCounter;
    }
}
