package org.safehaus.penrose.ldap.source;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.util.TextUtil;

import java.util.Date;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * @author Endi Sukma Dewata
 */
public class ADUserSource extends LDAPSource {

    protected boolean checkPassword = true;

    protected boolean checkAccountDisabled;
    protected boolean checkAccountLockout;
    protected boolean checkAccountExpires;

    public void init() throws Exception {
        super.init();

        String s = getParameter("checkPassword");
        if (s != null) checkPassword = Boolean.parseBoolean(s);

        checkAccountDisabled = Boolean.parseBoolean(getParameter("checkAccountDisabled"));
        checkAccountLockout = Boolean.parseBoolean(getParameter("checkAccountLockout"));
        checkAccountExpires = Boolean.parseBoolean(getParameter("checkAccountExpires"));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        SearchResult sr = find(request.getDn());
        Attributes attributes = sr.getAttributes();

        bind(session, request, response, attributes);
    }

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response,
            Attributes attributes
    ) throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("AD User Bind "+getName(), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        log.debug("Checking Active Directory account.");

        long userAccountControl = Long.parseLong((String) attributes.getValue("userAccountControl"));
        log.debug("User account control: "+userAccountControl);

        if (checkAccountDisabled && (userAccountControl & 0x0002) > 0) {
            if (debug) log.debug("Account is disabled.");
            throw LDAP.createException(LDAP.INVALID_CREDENTIALS);

        } else {
            if (debug) log.debug("Account is enabled.");
        }

        if (checkAccountLockout && (userAccountControl & 0x0010) > 0) {
            if (debug) log.debug("Account is locked out.");
            throw LDAP.createException(LDAP.INVALID_CREDENTIALS);

        } else {
            if (debug) log.debug("Account is not locked out.");
        }

        if (checkAccountExpires) {
            long accountExpires = Long.parseLong((String)attributes.getValue("accountExpires"));
            if (debug) log.debug("accountExpires: "+accountExpires);

            if (accountExpires != 0 && accountExpires != 0x7FFFFFFFFFFFFFFFL) {

                Date d = toDate(accountExpires);

                if (d.getTime() < System.currentTimeMillis()) {
                    if (debug) log.debug("Account has expired at "+d+".");
                    throw LDAP.createException(LDAP.INVALID_CREDENTIALS);

                } else {
                    if (debug) log.debug("Account has not expired.");
                }
            }
        }

        if (checkPassword) {
            if (debug) log.debug("Binding to Active Directory account.");

            super.bind(session, request, response);
        }
    }

    public Date toDate(long accountExpires) {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(1601, 0, 1, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        accountExpires = accountExpires / 10000 + calendar.getTime().getTime();
        return new Date(accountExpires);
    }
}
