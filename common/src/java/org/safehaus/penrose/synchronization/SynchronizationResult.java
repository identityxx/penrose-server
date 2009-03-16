package org.safehaus.penrose.synchronization;

import java.io.Serializable;
import java.io.StringWriter;
import java.io.PrintWriter;

/**
 * @author Endi Sukma Dewata
 */
public class SynchronizationResult implements Serializable {

    public final static long serialVersionUID = 1L;

    private long sourceEntries;

    private long addedEntries;
    private long modifiedEntries;
    private long deletedEntries;
    private long unchangedEntries;
    private long failedEntries;

    private long targetEntries;

    private long duration;

    public SynchronizationResult() {
    }

    public void add(SynchronizationResult result) {

        sourceEntries += result.sourceEntries;
        addedEntries += result.addedEntries;
        modifiedEntries += result.modifiedEntries;
        deletedEntries += result.deletedEntries;
        unchangedEntries += result.unchangedEntries;
        failedEntries += result.failedEntries;
        targetEntries += result.targetEntries;
        duration += result.duration;
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
        targetEntries++;
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
        targetEntries--;
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

    public long getSourceEntries() {
        return sourceEntries;
    }

    public void setSourceEntries(long sourceEntries) {
        this.sourceEntries = sourceEntries;
    }

    public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter out = new PrintWriter(sw, true);

        out.println("Synchronization result:");
        out.println(" - source    : "+sourceEntries);
        out.println(" - added     : "+addedEntries);
        out.println(" - modified  : "+modifiedEntries);
        out.println(" - deleted   : "+deletedEntries);
        out.println(" - unchanged : "+unchangedEntries);
        out.println(" - failed    : "+failedEntries);
        out.println(" - target    : "+targetEntries);
        out.print  (" - duration  : "+(duration/1000.0)+" s");
        out.close();

        return sw.toString();
    }

    public long getTargetEntries() {
        return targetEntries;
    }

    public void setTargetEntries(long targetEntries) {
        this.targetEntries = targetEntries;
    }
}
