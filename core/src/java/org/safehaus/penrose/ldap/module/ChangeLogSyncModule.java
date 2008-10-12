package org.safehaus.penrose.ldap.module;

import org.safehaus.penrose.filter.AndFilter;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.NotFilter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.jdbc.QueryResponse;
import org.safehaus.penrose.jdbc.source.JDBCSource;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.source.LDAPSource;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.SourceManager;
import org.safehaus.penrose.util.BinaryUtil;

import java.io.BufferedReader;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * @author Endi Sukma Dewata
 */
public class ChangeLogSyncModule extends Module {

    LDAPSource source;
    LDAPSource target;
    LDAPSource changelog;
    JDBCSource tracker;

    public void init() throws Exception {

        log.debug("Initializing "+this.getName()+" module...");

        SourceManager sourceManager = partition.getSourceManager();

        String sourceName = getParameter("source");
        log.debug("Source: "+sourceName);
        source = (LDAPSource)sourceManager.getSource(sourceName);

        String targetName = getParameter("target");
        log.debug("Target: "+targetName);
        target = (LDAPSource)sourceManager.getSource(targetName);

        String changeLogName = getParameter("changelog");
        log.debug("Change Log: "+changeLogName);
        changelog = (LDAPSource)sourceManager.getSource(changeLogName);

        String trackerName = getParameter("tracker");
        log.debug("Tracker: "+trackerName);
        tracker = (JDBCSource)sourceManager.getSource(trackerName);
    }

    public void load() throws Exception {

        log.debug("==================================================================================");
        log.debug("Loading cache...");

        final Session session = createAdminSession();

        try {
            target.clear(session);
            tracker.clear(session);

            SearchRequest request = new SearchRequest();

            SearchResponse response = new SearchResponse() {
                public void add(SearchResult result) throws Exception {
                    target.add(session, result.getDn(), result.getAttributes());
                }
            };

            source.search(session, request, response);

            Long lastChangeNumber = getLastChangeNumber(session);
            if (lastChangeNumber != null) addTracker(session, lastChangeNumber);

        } finally {
            session.close();
        }
    }

    public void synchronize() throws Exception {

        log.debug("============================================================================================");
        log.debug("Synchronizing cache...");

        Session session = createAdminSession();

        try {
            Long lastTrackedNumber = getLastTrackedNumber(session);

            SearchRequest request = createSearchRequest(lastTrackedNumber);
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

                process(session, attributes);

                Long newChangeNumber = Long.parseLong(attributes.getValue("changeNumber").toString());
                addTracker(session, newChangeNumber);

            } while (response.hasNext());

            log.debug("LDAP synchronization completed.");

        } finally {
            session.close();
        }
    }

    public Long getLastChangeNumber(Session session) throws Exception {

        SearchRequest request = new SearchRequest();
        request.setAttributes(new String[] { "changeNumber" });

        SearchResponse response = new SearchResponse();

        changelog.search(session, request, response);
        
        Long lastChangeNumber = null;
        while (response.hasNext()) {
            SearchResult result = response.next();
            Attributes attributes = result.getAttributes();
            lastChangeNumber = Long.parseLong(attributes.getValue("changeNumber").toString());
        }

        return lastChangeNumber;
    }

    public Long getLastTrackedNumber(Session session) throws Exception {

        QueryResponse response = new QueryResponse() {
            public void add(Object object) throws Exception {
                ResultSet rs = (ResultSet)object;
                super.add(rs.getLong(1));
            }
        };

        String tableName = tracker.getTableName();
        tracker.executeQuery(session, "select max(changeNumber) from "+tableName, response);

        if (!response.hasNext()) return null;

        return (Long)response.next();
    }

    public void addTracker(Session session, Number changeNumber) throws Exception {

        Attributes attributes = new Attributes();
        attributes.setValue("changeNumber", changeNumber);
        attributes.setValue("changeTimestamp", new Timestamp(System.currentTimeMillis()));

        tracker.add(session, new DN(), attributes);
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
        if (debug) log.debug("Entry: "+targetDn);

        DN baseDn = source.getBaseDn();
        int scope = source.getScope();

        if (scope == SearchRequest.SCOPE_BASE) {
            if (!baseDn.matches(targetDn)) {
                if (debug) log.debug("Entry is not "+baseDn+".");
                return;
            }

        } else if (scope == SearchRequest.SCOPE_ONE) {
            DN parentDn = targetDn.getParentDn();
            if (debug) log.debug("Parent: "+parentDn+".");
            if (!baseDn.matches(parentDn)) {
                if (debug) log.debug("Entry is not a child of "+baseDn+".");
                return;
            }

        } else { // if (scope == SourceRequest.SCOPE_SUB) {
            if (!targetDn.endsWith(baseDn)) {
                if (debug) log.debug("Entry is not under "+baseDn+".");
                return;
            }
        }
        
        String changeType = (String)attributes.getValue("changeType");

        if ("add".equalsIgnoreCase(changeType)) {

            if (debug) log.debug("Adding "+targetDn);

            String changes = (String)attributes.getValue("changes");
            Attributes newAttributes = createAttributes(changes);

            if (debug) newAttributes.print();

            AddRequest request = new AddRequest();
            request.setDn(targetDn.getPrefix(baseDn));
            request.setAttributes(newAttributes);

            AddResponse response = new AddResponse();

            target.add(session, request, response);

        } else if ("modify".equalsIgnoreCase(changeType)) {

            if (debug) log.debug("Modifying "+targetDn);

            String changes = (String)attributes.getValue("changes");
            Collection<Modification> modifications = createModifications(changes);

            ModifyRequest request = new ModifyRequest();
            request.setDn(targetDn.getPrefix(baseDn));
            request.setModifications(modifications);

            ModifyResponse response = new ModifyResponse();

            target.modify(session, request, response);

        } else if ("modrdn".equalsIgnoreCase(changeType)) {

            if (debug) log.debug("Renaming "+targetDn);

            String newRdn = (String)attributes.getValue("newRDN");
            boolean deleteOldRdn = Boolean.parseBoolean((String)attributes.getValue("deleteOldRDN"));

            ModRdnRequest request = new ModRdnRequest();
            request.setDn(targetDn.getPrefix(baseDn));
            request.setNewRdn(newRdn);
            request.setDeleteOldRdn(deleteOldRdn);

            ModRdnResponse response = new ModRdnResponse();

            target.modrdn(session, request, response);

        } else if ("delete".equalsIgnoreCase(changeType)) {

            if (debug) log.debug("Deleting "+targetDn);

            DeleteRequest request = new DeleteRequest();
            request.setDn(targetDn.getPrefix(baseDn));

            DeleteResponse response = new DeleteResponse();

            target.delete(session, request, response);
        }
    }

    public Attributes createAttributes(String changes) throws Exception {
        Attributes attributes = new Attributes();

        BufferedReader in = new BufferedReader(new StringReader(changes));

        SchemaManager schemaManager = partition.getSchemaManager();

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

                boolean operational = schemaManager.isOperational(attributeName);
                if (!operational) attributes.addValue(attributeName, value);
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

            boolean operational = schemaManager.isOperational(attributeName);
            if (!operational) attributes.addValue(attributeName, value);
        }

        return attributes;
    }

    public Collection<Modification> createModifications(String changes) throws Exception {
        Collection<Modification> modifications = new ArrayList<Modification>();

        BufferedReader in = new BufferedReader(new StringReader(changes));

        SchemaManager schemaManager = partition.getSchemaManager();

        int operation = 0;
        String attributeName;
        boolean binary = false;
        StringBuilder sb = null;
        Attribute attribute = null;

        String line;
        while ((line = in.readLine()) != null) {

            if (debug) log.debug("Parsing ["+line+"]");

            line = line.trim();
            if (line.length() == 0) continue;

            if (line.startsWith(" ") && sb != null) {
                sb.append(line.substring(1));
                continue;
            }

            if (line.equals("-") && attribute != null && sb != null) {
                String s = sb.toString().trim();
                Object value = binary ? BinaryUtil.decode(BinaryUtil.BIG_INTEGER, s) : s;

                attribute.addValue(value);

                boolean operational = schemaManager.isOperational(attribute.getName());
                if (!operational) {
                    Modification modification = new Modification(operation, attribute);
                    modifications.add(modification);
                }

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

    public void update() throws Exception {

        log.debug("============================================================================================");
        log.debug("Updating cache...");

        Session session = createAdminSession();

        try {
            final Collection<DN> results1 = new TreeSet<DN>();
            final Collection<DN> results2 = new TreeSet<DN>();

            SearchRequest request1 = new SearchRequest();
            request1.setAttributes(new String[] { "dn" });

            SearchResponse response1 = new SearchResponse() {
                public void add(SearchResult result) throws Exception {
                    results1.add(result.getDn());
                }
            };

            target.search(session, request1, response1);

            SearchRequest request2 = new SearchRequest();
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

                    DeleteResponse response = new DeleteResponse();
                    target.delete(session, request, response);

                    b1 = i1.hasNext();
                    if (b1) dn1 = i1.next();

                } else if (c > 0) { // add new entry
                    SearchResult result2 = source.find(session, dn2);

                    AddRequest request = new AddRequest();
                    request.setDn(dn2);
                    request.setAttributes(result2.getAttributes());

                    AddResponse response = new AddResponse();
                    target.add(session, request, response);

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

                        ModifyResponse response = new ModifyResponse();
                        target.modify(session, request, response);
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

                DeleteResponse response = new DeleteResponse();
                target.delete(session, request, response);

                b1 = i1.hasNext();
                if (b1) dn1 = i1.next();
            }

            while (b2) { // add new entries
                SearchResult result2 = source.find(session, dn2);

                AddRequest request = new AddRequest();
                request.setDn(dn2);
                request.setAttributes(result2.getAttributes());

                AddResponse response = new AddResponse();
                target.add(session, request, response);

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

    public void clear() throws Exception {

        log.debug("==================================================================================");
        log.debug("Clearing cache...");

        Session session = createAdminSession();

        try {
            target.clear(session);
            tracker.clear(session);

        } finally {
            session.close();
        }
    }
}