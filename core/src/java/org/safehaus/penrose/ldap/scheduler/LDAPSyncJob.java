package org.safehaus.penrose.ldap.scheduler;

import org.safehaus.penrose.scheduler.Job;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.AndFilter;
import org.safehaus.penrose.filter.NotFilter;
import org.safehaus.penrose.util.BinaryUtil;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.jdbc.adapter.JDBCAdapter;
import org.safehaus.penrose.jdbc.JDBCClient;
import org.safehaus.penrose.jdbc.QueryResponse;
import org.safehaus.penrose.directory.Directory;
import org.safehaus.penrose.directory.Entry;

import java.io.BufferedReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.ArrayList;
import java.sql.Timestamp;
import java.sql.ResultSet;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPSyncJob extends Job {

    Source source;
    Source target;
    Source changelog;
    Source tracker;
    Source errors;

    public void init() throws Exception {
        String sourceName    = jobConfig.getParameter("source");
        String targetName    = jobConfig.getParameter("target");
        String changeLogName = jobConfig.getParameter("changelog");
        String trackerName   = jobConfig.getParameter("tracker");
        String errorsName    = jobConfig.getParameter("errors");

        Partition partition = getPartition();

        source    = partition.getSource(sourceName);
        target    = partition.getSource(targetName);
        changelog = partition.getSource(changeLogName);
        tracker   = partition.getSource(trackerName);
        errors    = partition.getSource(errorsName);
    }

    public void execute() throws Exception {
        synchronize();
    }

    public void create() throws Exception {
        Partition partition = getPartition();
        Directory directory = partition.getDirectory();

        Entry entry = directory.getRootEntries().iterator().next();
        create(entry);

        for (Entry child : entry.getChildren()) {
            create(child);
        }
    }
    
    public void create(String dn) throws Exception {
        create(new DN(dn));
    }

    public void create(DN dn) throws Exception {
        Partition partition = getPartition();
        Directory directory = partition.getDirectory();

        Entry entry = directory.getEntries(dn).iterator().next();
        create(entry);
    }

    public void create(Entry entry) throws Exception {
        DN dn = entry.getDn();
        Attributes attributes = entry.getAttributes();

        try {
            log.debug("Adding "+dn);
            target.add(dn, attributes);

        } catch (Throwable e) {

            if (errors == null) {
                throw new Exception(e);

            } else {

                Attributes attrs = new Attributes();
                attrs.setValue("time", new Timestamp(System.currentTimeMillis()));
                attrs.setValue("title", "Error creating "+dn);

                StringBuilder sb = new StringBuilder();
                sb.append("The following entry cannot be created:\n\n");

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

                errors.add(new DN(), attrs);
            }
        }
    }

    public void load() throws Exception {
        load((DN)null);
    }

    public void load(String baseDn) throws Exception {
        load(new DN(baseDn));
    }

    public void load(final DN baseDn) throws Exception {
        if (source == null) throw new Exception("Source not defined.");

        SearchRequest request = new SearchRequest();
        request.setDn(baseDn);

        SearchResponse response = new SearchResponse() {
            public void add(SearchResult result) throws Exception {

                DN dn = result.getDn();
                if (baseDn.equals(dn)) return;

                Attributes attributes = result.getAttributes();

                try {
                    log.debug("Adding "+dn);
                    target.add(dn, attributes);

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

                        errors.add(new DN(), attrs);
                    }
                }
            }
        };

        source.search(request, response);
    }

    public void clear() throws Exception {
        clear((DN)null);
    }

    public void clear(String baseDn) throws Exception {
        clear(new DN(baseDn));
    }

    public void clear(final DN baseDn) throws Exception {
        final ArrayList<DN> dns = new ArrayList<DN>();

        SearchRequest request = new SearchRequest();
        request.setDn(baseDn);

        SearchResponse response = new SearchResponse() {
            public void add(SearchResult result) throws Exception {
                DN dn = result.getDn();
                if (baseDn.equals(dn)) return;
                dns.add(dn);
            }
        };

        target.search(request, response);

        for (int i=dns.size()-1; i>=0; i--) {
            DN dn = dns.get(i);

            try {
                log.debug("Deleting "+dn);
                target.delete(dn);

            } catch (Throwable e) {

                if (errors == null) {
                    throw new Exception(e);

                } else {

                    Attributes attrs = new Attributes();
                    attrs.setValue("time", new Timestamp(System.currentTimeMillis()));
                    attrs.setValue("title", "Error deleting "+dn);

                    StringBuilder sb = new StringBuilder();
                    sb.append("The following entry cannot be deleted:\n\n");

                    sb.append(dn);
                    sb.append("\n\n");

                    StringWriter sw = new StringWriter();
                    e.printStackTrace(new PrintWriter(sw, true));

                    sb.append("Exception:\n");
                    sb.append(sw);

                    attrs.setValue("description", sb.toString());

                    errors.add(new DN(), attrs);
                }
            }
        }
    }

    public void remove() throws Exception {
        Partition partition = getPartition();
        Directory directory = partition.getDirectory();

        Entry entry = directory.getRootEntries().iterator().next();

        for (Entry child : entry.getChildren()) {
            remove(child);
        }

        remove(entry);
    }

    public void remove(String dn) throws Exception {
        remove(new DN(dn));
    }

    public void remove(DN dn) throws Exception {
        Partition partition = getPartition();
        Directory directory = partition.getDirectory();

        Entry entry = directory.getEntries(dn).iterator().next();
        remove(entry);
    }

    public void remove(Entry entry) throws Exception {
        DN baseDn = entry.getDn();
        final ArrayList<DN> dns = new ArrayList<DN>();

        SearchRequest request = new SearchRequest();
        request.setDn(baseDn);

        SearchResponse response = new SearchResponse() {
            public void add(SearchResult result) throws Exception {
                DN dn = result.getDn();
                dns.add(dn);
            }
        };

        target.search(request, response);

        for (int i=dns.size()-1; i>=0; i--) {
            DN dn = dns.get(i);

            try {
                log.debug("Deleting "+dn);
                target.delete(dn);

            } catch (Throwable e) {

                if (errors == null) {
                    throw new Exception(e);

                } else {

                    Attributes attrs = new Attributes();
                    attrs.setValue("time", new Timestamp(System.currentTimeMillis()));
                    attrs.setValue("title", "Error deleting "+dn);

                    StringBuilder sb = new StringBuilder();
                    sb.append("The following entry cannot be deleted:\n\n");

                    sb.append(dn);
                    sb.append("\n\n");

                    StringWriter sw = new StringWriter();
                    e.printStackTrace(new PrintWriter(sw, true));

                    sb.append("Exception:\n");
                    sb.append(sw);

                    attrs.setValue("description", sb.toString());

                    errors.add(new DN(), attrs);
                }
            }
        }
    }

    public void synchronize() throws Exception {

        Long changeNumber = getLastChangeNumber();

        SearchRequest request = createSearchRequest(changeNumber);
        SearchResponse response = new SearchResponse();

        changelog.search(request, response);

        if (!response.hasNext()) {
            if (debug) log.debug("There is no new changes.");
            return;
        }

        do {
            SearchResult result = response.next();
            DN dn = result.getDn();
            Attributes attributes = result.getAttributes();

            if (debug) {
                log.debug("Processing: "+dn);
                attributes.print();
            }

            try {
                process(attributes);

            } catch (Throwable e) {

                if (errors != null) {

                    Attributes attrs = new Attributes();
                    attrs.setValue("time", new Timestamp(System.currentTimeMillis()));
                    attrs.setValue("title", "Error processing "+dn);

                    StringBuilder sb = new StringBuilder();
                    sb.append("The following change log cannot be processed:\n\n");

                    sb.append(dn);
                    sb.append("\n\n");

                    StringWriter sw = new StringWriter();
                    e.printStackTrace(new PrintWriter(sw, true));

                    sb.append("Exception:\n");
                    sb.append(sw);

                    attrs.setValue("description", sb.toString());

                    errors.add(new DN(), attrs);
                }

                throw new Exception(e);
            }

            Long newChangeNumber = Long.parseLong(attributes.getValue("changeNumber").toString());

            addTracker(newChangeNumber);

        } while (response.hasNext());

        log.debug("LDAP synchronization completed.");
    }

    public Long getLastChangeNumber() throws Exception {

        Connection connection = tracker.getConnection();
        JDBCAdapter adapter = (JDBCAdapter)connection.getAdapter();
        JDBCClient client = adapter.getClient();

        QueryResponse response = new QueryResponse() {
            public void add(Object object) throws Exception {
                ResultSet rs = (ResultSet)object;
                super.add(rs.getLong(1));
            }
        };

        client.executeQuery("select max(changeNumber) from "+client.getTableName(tracker), response);

        if (!response.hasNext()) return null;

        return (Long)response.next();
    }

    public void addTracker(Long changeNumber) throws Exception {

        Attributes attributes = new Attributes();
        attributes.setValue("changeNumber", changeNumber);
        attributes.setValue("changeTimestamp", new Timestamp(System.currentTimeMillis()));

        tracker.add(new DN(), attributes);
    }

    public void removeTracker(Long changeNumber) throws Exception {

        RDNBuilder rb = new RDNBuilder();
        rb.set("changeNumber", changeNumber);

        tracker.delete(new DN(rb.toRdn()));
    }

    public SearchRequest createSearchRequest(Number changeNumber) throws Exception {

        Filter changeLogFilter = null;

        if (changeNumber != null) {

            SimpleFilter sf1 = new SimpleFilter("changeNumber", ">=", changeNumber);
            SimpleFilter sf2 = new SimpleFilter("changeNumber", "=", changeNumber);

            AndFilter af = new AndFilter();
            af.addFilter(sf1);
            af.addFilter(new NotFilter(sf2));

            changeLogFilter = af;
        }

        SearchRequest request = new SearchRequest();
        request.setFilter(changeLogFilter);

        return request;
    }

    public void process(Attributes attributes) throws Exception {

        DN targetDn = new DN((String)attributes.getValue("targetDN"));

        String changeType = (String)attributes.getValue("changeType");

        if ("add".equals(changeType)) {

            if (debug) log.debug("Adding "+targetDn);

            String changes = (String)attributes.getValue("changes");
            Attributes newAttributes = parseAttributes(changes);

            if (debug) newAttributes.print();

            AddRequest request = new AddRequest();
            request.setDn(targetDn);
            request.setAttributes(newAttributes);

            AddResponse response = new AddResponse();

            target.add(request, response);

        } else if ("modify".equals(changeType)) {

            if (debug) log.debug("Modifying "+targetDn);

            String changes = (String)attributes.getValue("changes");
            Collection<Modification> modifications = parseModifications(changes);

            ModifyRequest request = new ModifyRequest();
            request.setDn(targetDn);
            request.setModifications(modifications);

            ModifyResponse response = new ModifyResponse();

            target.modify(request, response);

        } else if ("modrdn".equals(changeType)) {

            if (debug) log.debug("Renaming "+targetDn);

            String newRdn = (String)attributes.getValue("newRDN");
            boolean deleteOldRdn = Boolean.parseBoolean((String)attributes.getValue("deleteOldRDN"));

            ModRdnRequest request = new ModRdnRequest();
            request.setDn(targetDn);
            request.setNewRdn(newRdn);
            request.setDeleteOldRdn(deleteOldRdn);

            ModRdnResponse response = new ModRdnResponse();

            target.modrdn(request, response);

        } else if ("delete".equals(changeType)) {

            if (debug) log.debug("Deleting "+targetDn);

            DeleteRequest request = new DeleteRequest();
            request.setDn(targetDn);

            DeleteResponse response = new DeleteResponse();
            
            target.delete(request, response);
        }
    }

    public Attributes parseAttributes(String changes) throws Exception {
        Attributes attributes = new Attributes();

        BufferedReader in = new BufferedReader(new StringReader(changes));

        String attributeName = null;
        boolean binary = false;
        StringBuilder sb = new StringBuilder();

        String line;
        while ((line = in.readLine()) != null) {

            if (debug) log.debug("Parsing ["+line+"]");

            if (line.startsWith(" ")) {
                sb.append(line.substring(1));
                continue;
            }

            if (attributeName != null) {
                String s = sb.toString().trim();
                Object value = binary ? BinaryUtil.decode(BinaryUtil.BIG_INTEGER, s) : s;

                attributes.addValue(attributeName, value);
                sb = new StringBuilder();
            }

            int i = line.indexOf(":");
            attributeName = line.substring(0, i);

            i++;
            if (line.charAt(i) == ':') {
                binary = true;
                i++;

            } else {
                binary = false;
            }

            sb.append(line.substring(i));
        }

        if (attributeName != null) {
            String s = sb.toString().trim();
            Object value = binary ? BinaryUtil.decode(BinaryUtil.BIG_INTEGER, s) : s;

            attributes.addValue(attributeName, value);
        }

        return attributes;
    }

    public Collection<Modification> parseModifications(String changes) throws Exception {
        Collection<Modification> modifications = new ArrayList<Modification>();

        BufferedReader in = new BufferedReader(new StringReader(changes));

        int operation = 0;
        String attributeName;
        boolean binary = false;
        StringBuilder sb = null;
        Attribute attribute = null;

        String line;
        while ((line = in.readLine()) != null) {

            if (debug) log.debug("Parsing ["+line+"]");

            if (line.startsWith(" ") && sb != null) {
                sb.append(line.substring(1));
                continue;
            }

            if (line.equals("-") && attribute != null && sb != null) {
                String s = sb.toString().trim();
                Object value = binary ? BinaryUtil.decode(BinaryUtil.BIG_INTEGER, s) : s;

                attribute.addValue(value);

                Modification modification = new Modification(operation, attribute);
                modifications.add(modification);

                operation = 0;
                sb = null;
                continue;
            }

            int i = line.indexOf(":");

            if (operation == 0) {
                operation = LDAP.getModificationOperation(line.substring(0, i));
                attributeName = line.substring(i+1).trim();

                attribute = new Attribute(attributeName);
                continue;
            }

            if (sb != null) {
                String s = sb.toString().trim();
                Object value = binary ? BinaryUtil.decode(BinaryUtil.BIG_INTEGER, s) : s;

                attribute.addValue(value);
            }

            i++;

            if (line.charAt(i) == ':') {
                binary = true;
                i++;

            } else {
                binary = false;
            }

            sb = new StringBuilder();
            sb.append(line.substring(i));
        }

        return modifications;
    }
}
