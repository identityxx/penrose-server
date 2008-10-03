package org.safehaus.penrose.federation;

import java.io.Serializable;

/**
 * @author Endi Sukma Dewata
 */
public class SynchronizationResult implements Serializable {

    private long duration;

    private long addedEntries;
    private long modifiedEntries;
    private long deletedEntries;
    private long unchangedEntries;
    private long failedEntries;

    public long getTotalEntries() {
        return addedEntries+modifiedEntries+deletedEntries+unchangedEntries+failedEntries;
    }
    
    public void add(SynchronizationResult result) {

        duration += result.duration;

        addedEntries += result.addedEntries;
        modifiedEntries += result.modifiedEntries;
        deletedEntries += result.deletedEntries;
        unchangedEntries += result.unchangedEntries;
        failedEntries += result.failedEntries;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public long getAddedEntries() {
        return addedEntries;
    }

    public void setAddedEntries(long addedEntries) {
        this.addedEntries = addedEntries;
    }

    public void incAddedEntries() {
        addedEntries++;
    }

    public long getModifiedEntries() {
        return modifiedEntries;
    }

    public void setModifiedEntries(long modifiedEntries) {
        this.modifiedEntries = modifiedEntries;
    }

    public void incModifiedEntries() {
        modifiedEntries++;
    }

    public long getDeletedEntries() {
        return deletedEntries;
    }

    public void setDeletedEntries(long deletedEntries) {
        this.deletedEntries = deletedEntries;
    }

    public void incDeletedEntries() {
        deletedEntries++;
    }

    public long getUnchangedEntries() {
        return unchangedEntries;
    }

    public void setUnchangedEntries(long unchangedEntries) {
        this.unchangedEntries = unchangedEntries;
    }

    public void incUnchangedEntries() {
        unchangedEntries++;
    }

    public long getFailedEntries() {
        return failedEntries;
    }

    public void setFailedEntries(long failedEntries) {
        this.failedEntries = failedEntries;
    }

    public void incFailedEntries() {
        failedEntries++;
    }
}
