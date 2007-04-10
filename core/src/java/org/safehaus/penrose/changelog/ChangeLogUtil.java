package org.safehaus.penrose.changelog;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.source.Source;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public abstract class ChangeLogUtil {

    public Logger log = LoggerFactory.getLogger(getClass());

    protected Source source;
    protected Source destination;
    protected Source changeLog;
    protected Source tracker;

    protected String user;

    public void update() throws Exception {

        boolean debug = log.isDebugEnabled();

        Number changeNumber = getLastChangeNumber();

        SearchRequest request = createSearchRequest(changeNumber);

        SearchResponse response = new SearchResponse() {
            public void add(Object object) throws Exception {
                Entry entry = (Entry)object;
                super.add(createChangeLog(entry));
            }
        };

        changeLog.search(request, response);

        if (!response.hasNext()) {
            if (debug) log.debug("There is no new changes.");
            return;
        }

        do {
            ChangeLog changeLog = (ChangeLog)response.next();
            execute(changeLog);

            Number cn = changeLog.getChangeNumber();

            if (changeNumber == null) {
                setLastChangeNumber(cn);
                changeNumber = cn;

            } else {
                updateLastChangeNumber(cn);
            }

        } while (response.hasNext());

        log.debug("Cache synchronization completed.");
    }

    public abstract SearchRequest createSearchRequest(Number changeNumber) throws Exception;

    public abstract ChangeLog createChangeLog(Entry changeLogEntry) throws Exception;

    public Number getLastChangeNumber() throws Exception {

        boolean debug = log.isDebugEnabled();

        RDNBuilder rb = new RDNBuilder();
        rb.set("sourceName", source.getName());

        DN dn = new DN(rb.toRdn());

        SearchResponse response = tracker.search(dn, null, SearchRequest.SCOPE_BASE);

        if (!response.hasNext()) return null;

        Entry trackerEntry = (Entry)response.next();
        Attributes attributes = trackerEntry.getAttributes();

        if (debug) {
            log.debug("Tracker: "+trackerEntry.getDn());
            attributes.print();
        }

        return (Number)attributes.getValue("changeNumber");
    }

    public void setLastChangeNumber(Number changeNumber) throws Exception {

        RDNBuilder rb = new RDNBuilder();
        rb.set("sourceName", source.getName());

        DN dn = new DN(rb.toRdn());

        Attributes attributes = new Attributes();
        attributes.setValue("sourceName", source.getName());
        attributes.setValue("changeNumber", changeNumber);

        tracker.add(dn, attributes);
    }

    public void updateLastChangeNumber(Number changeNumber) throws Exception {

        RDNBuilder rb = new RDNBuilder();
        rb.set("sourceName", source.getName());

        Attribute attributes = new Attribute("changeNumber");
        attributes.setValue(changeNumber);

        Modification modification = new Modification(Modification.REPLACE, attributes);

        ModifyRequest request = new ModifyRequest();
        request.setDn(rb.toRdn());
        request.addModification(modification);

        ModifyResponse response = new ModifyResponse();

        tracker.modify(request, response);
    }

    public void execute(ChangeLog changeLog) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (user != null && user.equals(changeLog.getChangeUser())) {
            if (debug) log.debug("Ignore changes from "+user);
            return;
        }

        switch (changeLog.getChangeAction()) {
            case ChangeLog.ADD:
                AddRequest addRequest = (AddRequest)changeLog.getRequest();
                AddResponse addResponse = new AddResponse();
                destination.add(addRequest, addResponse);
                break;

            case ChangeLog.MODIFY:
                ModifyRequest modifyRequest = (ModifyRequest)changeLog.getRequest();
                ModifyResponse modifyResponse = new ModifyResponse();
                destination.modify(modifyRequest, modifyResponse);
                break;

            case ChangeLog.MODRDN:
                ModRdnRequest modRdnRequest = (ModRdnRequest)changeLog.getRequest();
                ModRdnResponse modRdnResponse = new ModRdnResponse();
                destination.modrdn(modRdnRequest, modRdnResponse);
                break;

            case ChangeLog.DELETE:
                DeleteRequest deleteRequest = (DeleteRequest)changeLog.getRequest();
                DeleteResponse deleteResponse = new DeleteResponse();
                destination.delete(deleteRequest, deleteResponse);
                break;
        }
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public Source getDestination() {
        return destination;
    }

    public void setDestination(Source destination) {
        this.destination = destination;
    }

    public Source getChangeLog() {
        return changeLog;
    }

    public void setChangeLog(Source changeLog) {
        this.changeLog = changeLog;
    }

    public Source getTracker() {
        return tracker;
    }

    public void setTracker(Source tracker) {
        this.tracker = tracker;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }
}
