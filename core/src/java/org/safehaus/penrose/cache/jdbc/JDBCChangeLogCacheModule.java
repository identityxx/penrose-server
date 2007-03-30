package org.safehaus.penrose.cache.jdbc;

import org.safehaus.penrose.cache.ChangeLog;
import org.safehaus.penrose.cache.ChangeLogCacheModule;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.connection.Connection;

import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class JDBCChangeLogCacheModule extends ChangeLogCacheModule {

    public String getUser() {
        Connection connection = source.getConnection();
        return connection.getParameter("user");
    }

    public SearchRequest createSearchRequest(Number changeNumber) throws Exception {

        SearchRequest request = new SearchRequest();

        if (changeNumber != null) {
            Filter filter = new SimpleFilter("changeNumber", ">", changeNumber);
            request.setFilter(filter);
        }

        return request;
    }

    public ChangeLog createChangeLog(Entry changeLogEntry) throws Exception {

        boolean debug = log.isDebugEnabled();

        Attributes changeLogAttributes = changeLogEntry.getAttributes();

        if (debug) {
            log.debug("Change Log: "+changeLogEntry.getDn());
            changeLogAttributes.print();
        }

        ChangeLog changeLog = new ChangeLog();
        changeLog.setChangeNumber((Number)changeLogAttributes.getValue("changeNumber"));
        changeLog.setChangeTime(changeLogAttributes.getValue("changeTime"));
        changeLog.setChangeUser((String)changeLogAttributes.getValue("changeUser"));

        RDNBuilder rb = new RDNBuilder();
        Attributes attributes = new Attributes();

        for (Iterator i= source.getFields().iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();
            String fieldName = field.getName();

            Object value = changeLogAttributes.getValue(fieldName);

            attributes.addValue(fieldName, value);
            if (field.isPrimaryKey()) rb.set(fieldName, value);
        }

        DN dn = new DN(rb.toRdn());

        String changeAction = (String)changeLogAttributes.getValue("changeAction");

        if ("ADD".equals(changeAction)) {

            AddRequest request = new AddRequest();
            request.setDn(dn);
            request.setAttributes(attributes);

            changeLog.setRequest(request);
            changeLog.setChangeAction(ChangeLog.ADD);

        } else if ("MODIFY".equals(changeAction)) {

            ModifyRequest request = new ModifyRequest();
            request.setDn(dn);
            for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
                Attribute attribute = (Attribute)i.next();
                request.addModification(new Modification(Modification.REPLACE, attribute));
            }

            changeLog.setRequest(request);
            changeLog.setChangeAction(ChangeLog.MODIFY);

        } else if ("DELETE".equals(changeAction)) {

            DeleteRequest request = new DeleteRequest();
            request.setDn(dn);

            changeLog.setRequest(request);
            changeLog.setChangeAction(ChangeLog.DELETE);
        }

        return changeLog;
    }
}
