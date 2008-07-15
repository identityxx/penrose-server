package org.safehaus.penrose.nis.module;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.directory.Directory;
import org.safehaus.penrose.session.Session;

import java.sql.Timestamp;
import java.io.StringWriter;
import java.io.PrintWriter;

/**
 * @author Endi Sukma Dewata
 */
public class NISDBSyncModule extends Module {

    String sourcePartitionName;

    Source errors;

    public void init() throws Exception {
        String sourcePartitionName = getParameter("source");
        log.debug("Source: "+sourcePartitionName);

        this.sourcePartitionName = sourcePartitionName;
    }

    public void addEntry(Session session, DN dn, Attributes attributes) throws Exception {

        try {
            log.debug("Adding "+dn);

            AddRequest request = new AddRequest();
            request.setDn(dn);
            request.setAttributes(attributes);

            AddResponse response = new AddResponse();

            partition.add(session, request, response);

        } catch (Throwable e) {

            if (errors == null) {
                throw new Exception(e);

            } else {

                Attributes attrs = new Attributes();
                attrs.setValue("time", new Timestamp(System.currentTimeMillis()));
                attrs.setValue("title", "Error loading "+dn);

                StringBuilder sb = new StringBuilder();
                sb.append("The following entry cannot be loaded:\n\n");

                sb.append("dn: ");
                sb.append(dn);
                sb.append("\n");
                sb.append(attributes);
                sb.append("\n\n");

                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw, true));

                sb.append("Exception:\n");
                sb.append(sw);

                attrs.setValue("description", sb.toString());

                errors.add(session, new DN(), attrs);
            }
        }
    }

    public void create(String targetDn) throws Exception {
        create(new DN(targetDn));
    }

    public void create(DN targetDn) throws Exception {

        log.debug("##################################################################################################");
        log.debug("Creating "+targetDn);

        Directory directory = partition.getDirectory();
        Entry entry = directory.findEntries(targetDn).iterator().next();

        for (Entry child : entry.getChildren()) {
            create(child);
        }
    }

    public void create(Entry entry) throws Exception {
        for (SourceRef sourceRef : entry.getSourceRefs()) {
            Source source = sourceRef.getSource();
            source.create();
        }
    }

    public void load(String targetDn) throws Exception {
        load(new DN(targetDn));
    }

    public void load(final DN targetDn) throws Exception {

        final Session adminSession = createAdminSession();

        try {
            Partition sourcePartition = partition.getPartitionContext().getPartition(sourcePartitionName);

            DN sourceSuffix = sourcePartition.getDirectory().getSuffix();
            DN targetSuffix = partition.getDirectory().getSuffix();

            log.debug("##################################################################################################");
            log.debug("Loading "+targetDn);

            final DN sourceDn = targetDn.getPrefix(targetSuffix).append(sourceSuffix);
            final int nisDnSize = sourceDn.getSize();

            SearchRequest request = new SearchRequest();
            request.setDn(sourceDn);

            SearchResponse response = new SearchResponse() {
                public void add(SearchResult result) throws Exception {

                    DN dn = result.getDn();
                    if (sourceDn.equals(dn)) return;

                    int dnSize = dn.getSize();
                    DN newDn = dn.getPrefix(dnSize - nisDnSize).append(targetDn);

                    Attributes attributes = result.getAttributes();

                    addEntry(adminSession, newDn, attributes);
                }
            };
    
            sourcePartition.search(adminSession, request, response);

            log.debug("Waiting for operation to complete.");
            int rc = response.waitFor();
            log.debug("RC: "+rc);

        } finally {
            adminSession.close();
        }
    }

    public void clear(String targetDn) throws Exception {
        clear(new DN(targetDn));
    }

    public void clear(DN targetDn) throws Exception {

        log.debug("##################################################################################################");
        log.debug("Clearing "+targetDn);

        Directory directory = partition.getDirectory();
        Entry entry = directory.findEntries(targetDn).iterator().next();

        for (Entry child : entry.getChildren()) {
            clear(child);
        }
    }

    public void clear(Entry entry) throws Exception {
        
        Session session = createAdminSession();

        try {
            for (SourceRef sourceRef : entry.getSourceRefs()) {
                Source source = sourceRef.getSource();
                source.clear(session);
            }

        } finally {
            session.close();
        }
    }

    public void remove(String targetDn) throws Exception {
        remove(new DN(targetDn));
    }

    public void remove(DN targetDn) throws Exception {

        log.debug("##################################################################################################");
        log.debug("Removing "+targetDn);

        Directory directory = partition.getDirectory();
        Entry entry = directory.findEntries(targetDn).iterator().next();

        for (Entry child : entry.getChildren()) {
            remove(child);
        }
    }

    public void remove(Entry entry) throws Exception {
        for (SourceRef sourceRef : entry.getSourceRefs()) {
            Source source = sourceRef.getSource();
            source.drop();
        }
    }

    public long getCount(String targetDn) throws Exception {
        return getCount(new DN(targetDn));
    }

    public long getCount(DN targetDn) throws Exception {

        log.debug("##################################################################################################");
        log.debug("Counting "+targetDn);

        Directory directory = partition.getDirectory();
        Entry entry = directory.findEntries(targetDn).iterator().next();

        Entry child = entry.getChildren().iterator().next();
        SourceRef sourceRef = child.getSourceRefs().iterator().next();
        Source source = sourceRef.getSource();

        return 0; // source.getCount();
    }
}
