package org.safehaus.penrose.ldap.scheduler;

import org.safehaus.penrose.scheduler.Job;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceManager;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.AndFilter;
import org.safehaus.penrose.filter.NotFilter;
import org.safehaus.penrose.util.BinaryUtil;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.jdbc.QueryResponse;
import org.safehaus.penrose.jdbc.connection.JDBCConnection;
import org.safehaus.penrose.directory.Directory;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.session.Session;

import java.io.BufferedReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.util.*;
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
        SourceManager sourceManager = partition.getSourceManager();

        source    = sourceManager.getSource(sourceName);
        target    = sourceManager.getSource(targetName);
        changelog = sourceManager.getSource(changeLogName);
        tracker   = sourceManager.getSource(trackerName);
        errors    = sourceManager.getSource(errorsName);
    }

    public void execute() throws Exception {
         synchronize();
    }

    public void create() throws Exception {

        Session session = getSession();

        try {
            Partition partition = getPartition();
            Directory directory = partition.getDirectory();

            Entry entry = directory.getRootEntries().iterator().next();
            create(session, entry);

            for (Entry child : entry.getChildren()) {
                create(session, child);
            }

        } finally {
            session.close();
        }
    }
    
    public void create(String dn) throws Exception {
        create(new DN(dn));
    }

    public void create(DN dn) throws Exception {

        Session session = getSession();

        try {
            Partition partition = getPartition();
            Directory directory = partition.getDirectory();

            Entry entry = directory.getEntries(dn).iterator().next();
            create(session, entry);

        } finally {
            session.close();
        }
    }

    public void create(Session session, Entry entry) throws Exception {

        Interpreter interpreter = partition.newInterpreter();

        DN dn = entry.computeDn(interpreter);
        Attributes attributes = entry.computeAttributes(interpreter);

        try {
            log.debug("Adding "+dn);
            target.add(session, dn, attributes);

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

                errors.add(session, new DN(), attrs);
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

        final Session session = getSession();

        try {
            SearchRequest request = new SearchRequest();
            request.setDn(baseDn);

            SearchResponse response = new SearchResponse() {
                public void add(SearchResult result) throws Exception {

                    DN dn = result.getDn();
                    if (baseDn.equals(dn)) return;

                    Attributes attributes = result.getAttributes();

                    try {
                        log.debug("Adding "+dn);
                        target.add(session, dn, attributes);

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
            };

            source.search(session, request, response);

        } finally {
            session.close();
        }
    }

    public void clear() throws Exception {
        clear((DN)null);
    }

    public void clear(String baseDn) throws Exception {
        clear(new DN(baseDn));
    }

    public void clear(final DN baseDn) throws Exception {

        Session session = getSession();

        try {
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

            target.search(session, request, response);

            for (int i=dns.size()-1; i>=0; i--) {
                DN dn = dns.get(i);

                try {
                    log.debug("Deleting "+dn);
                    target.delete(session, dn);

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

                        errors.add(session, new DN(), attrs);
                    }
                }
            }

        } finally {
            session.close();
        }
    }

    public void remove() throws Exception {

        final Session session = getSession();

        try {
            Partition partition = getPartition();
            Directory directory = partition.getDirectory();

            Entry entry = directory.getRootEntries().iterator().next();

            for (Entry child : entry.getChildren()) {
                remove(session, child);
            }

            remove(session, entry);

        } finally {
            session.close();
        }
    }

    public void remove(String dn) throws Exception {
        remove(new DN(dn));
    }

    public void remove(DN dn) throws Exception {

        final Session session = getSession();

        try {
            Partition partition = getPartition();
            Directory directory = partition.getDirectory();

            Entry entry = directory.getEntries(dn).iterator().next();
            remove(session, entry);

        } finally {
            session.close();
        }
    }

    public void remove(Session session, Entry entry) throws Exception {

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

        target.search(session, request, response);

        for (int i=dns.size()-1; i>=0; i--) {
            DN dn = dns.get(i);

            try {
                log.debug("Deleting "+dn);
                target.delete(session, dn);

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

                    errors.add(session, new DN(), attrs);
                }
            }
        }
    }
/*
    public void synchronize(final DN baseDn) throws Exception {
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
*/
    public void synchronize() throws Exception {

        Session session = getSession();

        try {
            Long changeNumber = getLastChangeNumber();

            SearchRequest request = createSearchRequest(changeNumber);
            SearchResponse response = new SearchResponse();

            changelog.search(session, request, response);

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
                    process(session, attributes);

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

                        errors.add(session, new DN(), attrs);
                    }

                    throw new Exception(e);
                }

                Long newChangeNumber = Long.parseLong(attributes.getValue("changeNumber").toString());

                addTracker(newChangeNumber);

            } while (response.hasNext());

            log.debug("LDAP synchronization completed.");

        } finally {
            session.close();
        }
    }

    public Long getLastChangeNumber() throws Exception {

        Connection connection = tracker.getConnection();
        JDBCConnection adapter = (JDBCConnection)connection;

        QueryResponse response = new QueryResponse() {
            public void add(Object object) throws Exception {
                ResultSet rs = (ResultSet)object;
                super.add(rs.getLong(1));
            }
        };

        adapter.executeQuery("select max(changeNumber) from "+adapter.getTableName(tracker), response);

        if (!response.hasNext()) return null;

        return (Long)response.next();
    }

    public void addTracker(Long changeNumber) throws Exception {

        Session session = getSession();

        try {
            Attributes attributes = new Attributes();
            attributes.setValue("changeNumber", changeNumber);
            attributes.setValue("changeTimestamp", new Timestamp(System.currentTimeMillis()));

            tracker.add(session, new DN(), attributes);

        } finally {
            session.close();
        }
    }

    public void removeTracker(Long changeNumber) throws Exception {

        Session session = getSession();

        try {
            RDNBuilder rb = new RDNBuilder();
            rb.set("changeNumber", changeNumber);

            tracker.delete(session, new DN(rb.toRdn()));

        } finally {
            session.close();
        }
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

    public void process(Session session, Attributes attributes) throws Exception {

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

            target.add(session, request, response);

        } else if ("modify".equals(changeType)) {

            if (debug) log.debug("Modifying "+targetDn);

            String changes = (String)attributes.getValue("changes");
            Collection<Modification> modifications = parseModifications(changes);

            ModifyRequest request = new ModifyRequest();
            request.setDn(targetDn);
            request.setModifications(modifications);

            ModifyResponse response = new ModifyResponse();

            target.modify(session, request, response);

        } else if ("modrdn".equals(changeType)) {

            if (debug) log.debug("Renaming "+targetDn);

            String newRdn = (String)attributes.getValue("newRDN");
            boolean deleteOldRdn = Boolean.parseBoolean((String)attributes.getValue("deleteOldRDN"));

            ModRdnRequest request = new ModRdnRequest();
            request.setDn(targetDn);
            request.setNewRdn(newRdn);
            request.setDeleteOldRdn(deleteOldRdn);

            ModRdnResponse response = new ModRdnResponse();

            target.modrdn(session, request, response);

        } else if ("delete".equals(changeType)) {

            if (debug) log.debug("Deleting "+targetDn);

            DeleteRequest request = new DeleteRequest();
            request.setDn(targetDn);

            DeleteResponse response = new DeleteResponse();
            
            target.delete(session, request, response);
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

    public void synchronize(DN dn) throws Exception {

        Session session = getSession();

        try {
            final Collection<DN> results1 = new TreeSet<DN>();
            final Collection<DN> results2 = new TreeSet<DN>();

            SearchRequest request1 = new SearchRequest();
            request1.setDn(dn);
            request1.setAttributes(new String[] { "dn" });

            SearchResponse response1 = new SearchResponse() {
                public void add(SearchResult result) throws Exception {
                    results1.add(result.getDn());
                }
            };

            target.search(session, request1, response1);

            SearchRequest request2 = new SearchRequest();
            request2.setDn(dn);
            request2.setAttributes(new String[] { "dn" });

            SearchResponse response2 = new SearchResponse() {
                public void add(SearchResult result) throws Exception {
                    results2.add(result.getDn());
                }
            };

            source.search(session, request2, response2);

            log.debug("Target:");
            for (DN d : results2) {
                log.debug(" - "+d);
            }

            log.debug("Source:");
            for (DN d : results1) {
                log.debug(" - "+d);
            }

            Iterator<DN> i1 = results1.iterator();
            Iterator<DN> i2 = results2.iterator();

            boolean b1 = i1.hasNext();
            boolean b2 = i2.hasNext();

            DN dn1 = b1 ? i1.next() : null;
            DN dn2 = b2 ? i2.next() : null;

            while (b1 && b2) {

                int c = dn1.compareTo(dn2);

                if (debug) log.debug("Comparing ["+dn1+"] with ["+dn2+"] => "+c);

                if (c < 0) { // delete old entry
                    DeleteRequest request = new DeleteRequest();
                    request.setDn(dn1);
                    execute(session, request);

                    b1 = i1.hasNext();
                    if (b1) dn1 = i1.next();

                } else if (c > 0) { // add new entry
                    SearchResult result2 = source.find(session, dn2);

                    AddRequest request = new AddRequest();
                    request.setDn(dn2);
                    request.setAttributes(result2.getAttributes());
                    execute(session, request);

                    b2 = i2.hasNext();
                    if (b2) dn2 = i2.next();

                } else {
                    SearchResult result1 = target.find(session, dn1);
                    SearchResult result2 = source.find(session, dn2);

                    Collection<Modification> modifications = createModifications(
                            result1.getAttributes(),
                            result2.getAttributes()
                    );

                    if (!modifications.isEmpty()) { // modify entry
                        ModifyRequest request = new ModifyRequest();
                        request.setDn(dn1);
                        request.setModifications(modifications);
                        execute(session, request);
                    }

                    b1 = i1.hasNext();
                    if (b1) dn1 = i1.next();

                    b2 = i2.hasNext();
                    if (b2) dn2 = i2.next();
                }
            }

            while (b1) { // delete old entries
                DeleteRequest request = new DeleteRequest();
                request.setDn(dn1);
                execute(session, request);

                b1 = i1.hasNext();
                if (b1) dn1 = i1.next();
            }

            while (b2) { // add new entries
                SearchResult result2 = source.find(session, dn2);

                AddRequest request = new AddRequest();
                request.setDn(dn2);
                request.setAttributes(result2.getAttributes());
                execute(session, request);

                b2 = i2.hasNext();
                if (b2) dn2 = i2.next();
            }

        } finally {
            session.close();
        }
    }

    public Collection<Modification> createModifications(
            Attributes attributes1,
            Attributes attributes2
    ) throws Exception {

        Collection<Modification> modifications = new ArrayList<Modification>();

        Collection<String> oldAttributes = new ArrayList<String>();
        oldAttributes.addAll(attributes1.getNormalizedNames());
        oldAttributes.removeAll(attributes2.getNormalizedNames());

        for (String name : oldAttributes) {
            Attribute oldAttribute = attributes1.get(name);
            modifications.add(new Modification(Modification.DELETE, oldAttribute));
        }

        Collection<String> newAttributes = new ArrayList<String>();
        newAttributes.addAll(attributes2.getNormalizedNames());
        newAttributes.removeAll(attributes1.getNormalizedNames());

        for (String name : newAttributes) {
            Attribute newAttribute = attributes2.get(name);
            modifications.add(new Modification(Modification.ADD, newAttribute));
        }

        for (Attribute attribute1 : attributes1.getAll()) {
            Attribute attribute2 = attributes2.get(attribute1.getName());
            if (attribute2 == null) continue;

            Collection<Modification> mods = createModifications(
                    attribute1,
                    attribute2
            );

            if (mods.isEmpty()) continue;

            modifications.addAll(mods);
        }

        return modifications;
    }

    public Collection<Modification> createModifications(
            Attribute attribute1,
            Attribute attribute2
    ) throws Exception {
        Collection<Modification> modifications = new ArrayList<Modification>();

        Attribute oldAttribute = (Attribute)attribute1.clone();
        oldAttribute.removeValues(attribute2.getValues());

        if (!oldAttribute.isEmpty()) {
            modifications.add(new Modification(Modification.DELETE, oldAttribute));
        }

        Attribute newAttribute = (Attribute)attribute2.clone();
        newAttribute.removeValues(attribute1.getValues());

        if (!newAttribute.isEmpty()) {
            modifications.add(new Modification(Modification.ADD, newAttribute));
        }

        return modifications;
    }

    public void execute(Session session, AddRequest request) throws Exception {

        try {
            AddResponse response = new AddResponse();
            target.add(session, request, response);

        } catch (Throwable e) {

            String title = "Error creating "+request.getDn();

            StringBuilder sb = new StringBuilder();
            sb.append("The following operation failed:\n\n");

            sb.append(request);
            sb.append("\n");

            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw, true));

            sb.append("Exception:\n");
            sb.append(sw);

            String description = sb.toString();

            recordException(session, e, title, description);
        }
    }

    public void execute(Session session, ModifyRequest request) throws Exception {

        try {
            ModifyResponse response = new ModifyResponse();
            target.modify(session, request, response);

        } catch (Throwable e) {

            String title = "Error modifying "+request.getDn();

            StringBuilder sb = new StringBuilder();
            sb.append("The following operation failed:\n\n");

            sb.append(request);
            sb.append("\n");

            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw, true));

            sb.append("Exception:\n");
            sb.append(sw);

            String description = sb.toString();

            recordException(session, e, title, description);
        }
    }

    public void execute(Session session, ModRdnRequest request) throws Exception {

        try {
            ModRdnResponse response = new ModRdnResponse();
            target.modrdn(session, request, response);

        } catch (Throwable e) {

            String title = "Error renaming "+request.getDn();

            StringBuilder sb = new StringBuilder();
            sb.append("The following operation failed:\n\n");

            sb.append(request);
            sb.append("\n");

            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw, true));

            sb.append("Exception:\n");
            sb.append(sw);

            String description = sb.toString();

            recordException(session, e, title, description);
        }
    }

    public void execute(Session session, DeleteRequest request) throws Exception {

        try {
            DeleteResponse response = new DeleteResponse();
            target.delete(session, request, response);

        } catch (Throwable e) {

            String title = "Error deleting "+request.getDn();

            StringBuilder sb = new StringBuilder();
            sb.append("The following operation failed:\n\n");

            sb.append(request);
            sb.append("\n");

            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw, true));

            sb.append("Exception:\n");
            sb.append(sw);

            String description = sb.toString();

            recordException(session, e, title, description);
        }
    }

    public void recordException(Session session, Throwable t, String title, String description) throws Exception {

        if (errors == null) throw new Exception(t);

        Attributes attrs = new Attributes();
        attrs.setValue("time", new Timestamp(System.currentTimeMillis()));
        attrs.setValue("title", title);
        attrs.setValue("description", description);

        errors.add(session, new DN(), attrs);
    }
}
