package org.safehaus.penrose.jdbc.module;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.jdbc.QueryResponse;
import org.safehaus.penrose.jdbc.source.JDBCSource;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.SourceManager;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class JDBCSyncModule extends Module {

    JDBCSource source;
    JDBCSource target;
    JDBCSource changelog;
    JDBCSource tracker;

    public void init() throws Exception {

        log.debug("Initializing "+this.getName()+" module...");

        SourceManager sourceManager = partition.getSourceManager();

        String sourceName = getParameter("source");
        log.debug("Source: "+sourceName);
        source = (JDBCSource)sourceManager.getSource(sourceName);

        String targetName = getParameter("target");
        log.debug("Target: "+targetName);
        target = (JDBCSource)sourceManager.getSource(targetName);

        String changeLogName = getParameter("changelog");
        log.debug("Change Log: "+changeLogName);
        changelog = (JDBCSource)sourceManager.getSource(changeLogName);

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

        log.debug("==================================================================================");
        log.debug("Synchronizing cache...");

        final Session session = createAdminSession();

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

        } finally {
            session.close();
        }
    }

    public Long getLastChangeNumber(Session session) throws Exception {

        QueryResponse response = new QueryResponse() {
            public void add(Object object) throws Exception {
                ResultSet rs = (ResultSet)object;
                super.add(rs.getLong(1));
            }
        };

        String tableName = changelog.getTableName();
        changelog.executeQuery(session, "select max(changeNumber) from "+tableName, response);

        if (!response.hasNext()) return null;

        return (Long)response.next();
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

    public SearchRequest createSearchRequest(Number changeNumber) throws Exception {

        Filter changeLogFilter = null;

        if (changeNumber != null) {
            changeLogFilter = new SimpleFilter("changeNumber", ">", changeNumber);
        }

        SearchRequest request = new SearchRequest();
        request.setFilter(changeLogFilter);

        return request;
    }

    public void process(Session session, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        for (String name : source.getPrimaryKeyNames()) {
            Object value = attributes.getValue(name);
            rb.set(name, value);
        }

        DN targetDn = new DN(rb.toRdn());

        String changeType = (String)attributes.getValue("changeAction");

        if ("add".equalsIgnoreCase(changeType)) {

            if (debug) log.debug("Adding "+targetDn);

            Attributes newAttributes = createAttributes(attributes);

            if (debug) newAttributes.print();

            AddRequest request = new AddRequest();
            request.setDn(targetDn);
            request.setAttributes(newAttributes);

            AddResponse response = new AddResponse();

            target.add(session, request, response);

        } else if ("modify".equalsIgnoreCase(changeType)) {

            if (debug) log.debug("Modifying "+targetDn);

            Collection<Modification> modifications = createModifications(attributes);

            ModifyRequest request = new ModifyRequest();
            request.setDn(targetDn);
            request.setModifications(modifications);

            ModifyResponse response = new ModifyResponse();

            target.modify(session, request, response);

        } else if ("delete".equalsIgnoreCase(changeType)) {

            if (debug) log.debug("Deleting "+targetDn);

            DeleteRequest request = new DeleteRequest();
            request.setDn(targetDn);

            DeleteResponse response = new DeleteResponse();

            target.delete(session, request, response);
        }
    }

    public Attributes createAttributes(Attributes attributes) throws Exception {
        Attributes newAttributes = new Attributes();

        for (String name : source.getFieldNames()) {
            Attribute attribute = attributes.get(name);
            newAttributes.set(attribute);
        }

        return newAttributes;
    }

    public Collection<Modification> createModifications(Attributes attributes) throws Exception {
        Collection<Modification> modifications = new ArrayList<Modification>();

        for (String name : source.getFieldNames()) {
            Attribute attribute = attributes.get(name);
            Modification modification = new Modification(Modification.REPLACE, attribute);
            modifications.add(modification);
        }

        return modifications;
    }

    public void addTracker(Session session, Number changeNumber) throws Exception {

        Attributes attributes = new Attributes();
        attributes.setValue("changeNumber", changeNumber);
        attributes.setValue("changeTimestamp", new Timestamp(System.currentTimeMillis()));

        tracker.add(session, new DN(), attributes);
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