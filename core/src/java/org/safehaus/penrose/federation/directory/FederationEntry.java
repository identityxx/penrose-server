package org.safehaus.penrose.federation.directory;

import org.safehaus.penrose.directory.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.source.LDAPSource;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.util.BinaryUtil;
import org.safehaus.penrose.interpreter.Interpreter;

import java.util.Date;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * @author Endi Sukma Dewata
 */
public class FederationEntry extends DynamicEntry {

    private boolean checkAccountDisabled;
    private boolean checkAccountLockout;
    private boolean checkAccountExpires;

    public void init() {
        checkAccountDisabled = Boolean.parseBoolean(getParameter("checkAccountDisabled"));
        checkAccountLockout = Boolean.parseBoolean(getParameter("checkAccountLockout"));
        checkAccountExpires = Boolean.parseBoolean(getParameter("checkAccountExpires"));
    }

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN nssDn = request.getDn();

        SearchResult sr = find(nssDn);
        SourceValues sv = sr.getSourceValues();

        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(sv);

        log.debug("Checking Active Directory link.");

        SourceRef adSourceRef = getSourceRef("a");
        Source adSource = adSourceRef.getSource();

        Filter filter = null;

        for (FieldRef adFieldRef : adSourceRef.getFieldRefs()) {

            Object value = interpreter.eval(adFieldRef);
            if (value instanceof byte[]) {
                log.debug(adFieldRef.getName()+": "+BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[])value));
            } else {
                log.debug(adFieldRef.getName()+": "+value);
            }
            if (value == null) continue;

            SimpleFilter sf = new SimpleFilter("objectGUID", "=", value);
            filter = FilterTool.appendAndFilter(filter, sf);
        }

        if (filter != null) {

            log.debug("Searching Active Directory account.");

            SearchRequest adRequest = new SearchRequest();
            adRequest.setFilter(filter);

            SearchResponse adResponse = new SearchResponse();

            adSource.search(session, adRequest, adResponse);

            SearchResult adResult = adResponse.next();
            Attributes adAttributes = adResult.getAttributes();

            log.debug("Checking Active Directory account.");

            long userAccountControl = Long.parseLong((String)adAttributes.getValue("userAccountControl"));
            log.debug("userAccountControl: "+userAccountControl);

            if (checkAccountDisabled && (userAccountControl & 0x0002) > 0) {
                log.debug("Account is disabled.");
                throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
            }
/*
            if (checkAccountLockout && (userAccountControl & 0x0010) > 0) {
                log.debug("Account is locked out.");
                throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
            }

            if (checkAccountExpires) {
                long accountExpires = Long.parseLong((String)adAttributes.getValue("accountExpires"));
                log.debug("accountExpires: "+accountExpires);

                if (accountExpires != 0 && accountExpires != 0x7FFFFFFFFFFFFFFFL) {
                    Date d = toDate(accountExpires);
                    if (d.getTime() < System.currentTimeMillis()) {
                        log.debug("Account has expired at "+d+".");
                        throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
                    }
                }
            }
*/
            String bind = adSourceRef.getBind();
            if (!SourceMapping.IGNORE.equals(bind)) {
                log.debug("Binding to Active Directory account.");

                BindRequest newRequest = (BindRequest)request.clone();
                newRequest.setDn(adResult.getDn());

                adSource.bind(session, newRequest, response);
            }

            if (SourceMapping.SUFFICIENT.equals(bind)) {
                return;
            }
        }

        Object userPassword = interpreter.get("g.userPassword");

        if (userPassword != null) {
            log.debug("Binding to Global Repository.");

            SourceRef sourceRef = getSourceRef("g");
            Source source = sourceRef.getSource();

            DN globalDn = new DN((String)interpreter.get("g.dn"));
            DN globalBaseDn = new DN(source.getParameter(LDAPSource.BASE_DN));

            BindRequest newRequest = (BindRequest)request.clone();
            newRequest.setDn(globalDn.getPrefix(globalBaseDn));

            source.bind(session, newRequest, response);

            return;
        }

        SourceRef sourceRef = getSourceRef("n");
        Source source = sourceRef.getSource();

        DN nisDn = new DN((String)interpreter.get("n.dn"));
        DN nisBaseDn = new DN(source.getParameter(LDAPSource.BASE_DN));

        log.debug("Binding to NIS Repository.");

        BindRequest newRequest = (BindRequest)request.clone();
        newRequest.setDn(nisDn.getPrefix(nisBaseDn));

        source.bind(session, newRequest, response);
    }

    public Date toDate(long accountExpires) {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(1601, 0, 1, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        accountExpires = accountExpires / 10000 + calendar.getTime().getTime();
        return new Date(accountExpires);
    }

    public boolean isCheckAccountExpires() {
        return checkAccountExpires;
    }

    public void setCheckAccountExpires(boolean checkAccountExpires) {
        this.checkAccountExpires = checkAccountExpires;
    }

    public boolean isCheckAccountLockout() {
        return checkAccountLockout;
    }

    public void setCheckAccountLockout(boolean checkAccountLockout) {
        this.checkAccountLockout = checkAccountLockout;
    }

    public boolean isCheckAccountDisabled() {
        return checkAccountDisabled;
    }

    public void setCheckAccountDisabled(boolean checkAccountDisabled) {
        this.checkAccountDisabled = checkAccountDisabled;
    }
}
