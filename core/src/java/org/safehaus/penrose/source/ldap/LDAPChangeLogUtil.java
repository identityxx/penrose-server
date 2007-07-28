package org.safehaus.penrose.source.ldap;

import org.safehaus.penrose.changelog.ChangeLogUtil;
import org.safehaus.penrose.changelog.ChangeLog;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.AndFilter;
import org.safehaus.penrose.filter.NotFilter;
import org.safehaus.penrose.source.Field;

/**
 * @author Endi S. Dewata
 */
public class LDAPChangeLogUtil extends ChangeLogUtil {

    public SearchRequest createSearchRequest(Number changeNumber) throws Exception {

        Filter changeLogFilter = null;

        if (changeNumber != null) {

            // "(&(changeNumber>="+changeNumber+")(!(changeNumber="+changeNumber+")))";

            SimpleFilter sf1 = new SimpleFilter("changeNumber", ">=", changeNumber);
            SimpleFilter sf2 = new SimpleFilter("changeNumber", "=", changeNumber);

            AndFilter af = new AndFilter();
            af.addFilter(sf1);
            af.addFilter(new NotFilter(sf2));

            changeLogFilter = af;
        }

        SearchRequest request = new SearchRequest();
        request.setFilter(changeLogFilter);
        request.setScope(SearchRequest.SCOPE_ONE);

        return request;
    }

    public ChangeLog createChangeLog(SearchResult changeLogEntry) throws Exception {

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

        for (Field field : source.getFields()) {
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
            for (Attribute attribute : attributes.getAll()) {
                request.addModification(new Modification(Modification.REPLACE, attribute));
            }

            changeLog.setRequest(request);
            changeLog.setChangeAction(ChangeLog.MODIFY);

        } else if ("MODRDN".equals(changeAction)) {

            ModRdnRequest request = new ModRdnRequest();
            request.setDn(dn);
            // request.setNewRdn(newRdn);
            // request.setDeleteOldRdn(deleteOldRdn);

            changeLog.setRequest(request);
            changeLog.setChangeAction(ChangeLog.MODRDN);

        } else if ("DELETE".equals(changeAction)) {

            DeleteRequest request = new DeleteRequest();
            request.setDn(dn);

            changeLog.setRequest(request);
            changeLog.setChangeAction(ChangeLog.DELETE);
        }

        return changeLog;
    }
}
