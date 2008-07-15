package org.safehaus.penrose.directory;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.naming.PenroseContext;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class LinkEntry extends Entry {

    public final static String PARTITION = "partition";
    public final static String ENTRY_DN  = "dn";
    public final static String ENTRY_ID  = "entryId";

    String partitionName;
    String entryDn;
    String entryId;

    public void init() throws Exception {
        partitionName = getParameter(PARTITION);
        entryDn       = getParameter(ENTRY_DN);
        entryId       = getParameter(ENTRY_ID);

        super.init();
    }

    public Entry getLink() throws Exception {

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        PartitionManager partitionManager = penroseContext.getPartitionManager();
        Partition p = partitionName == null ? partition : partitionManager.getPartition(partitionName);

        Directory directory = p.getDirectory();

        if (entryId != null) {
            Entry link = directory.getEntry(entryId);
            if (link == null) {
                throw new Exception("Link entry "+entryId+" not found in partition "+partitionName+".");
            }
            return link;
        }

        if (entryDn != null) {
            Collection<Entry> entries = directory.getEntries(new DN(entryDn));
            if (entries.isEmpty()) {
                throw new Exception("Link entry "+entryDn+" not found in partition "+partitionName+".");
            }
            return entries.iterator().next();
        }

        Collection<Entry> entries = directory.getEntries(getDn());
        if (entries.isEmpty()) {
            throw new Exception("Link entry "+entryDn+" not found in partition "+partitionName+".");
        }
        return entries.iterator().next();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            AddRequest request,
            AddResponse response
    ) throws Exception {
        Entry link = getLink();
        link.add(session, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response
    ) throws Exception {
        Entry link = getLink();
        link.bind(session, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {
        Entry link = getLink();
        link.compare(session, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {
        Entry link = getLink();
        link.delete(session, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {
        Entry link = getLink();
        link.modify(session, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {
        Entry link = getLink();
        link.modrdn(session, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {
        Entry link = getLink();
        link.search(session, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unbind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind(
            Session session,
            UnbindRequest request,
            UnbindResponse response
    ) throws Exception {
        Entry link = getLink();
        link.unbind(session, request, response);
    }
}
