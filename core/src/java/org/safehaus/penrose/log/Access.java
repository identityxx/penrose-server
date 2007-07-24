package org.safehaus.penrose.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.LDAP;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.filter.Filter;

/**
 * @author Endi Sukma Dewata
 */
public class Access {

    public static Logger log = LoggerFactory.getLogger(Access.class);

    public static void log(Session session, AddRequest request) {

        if (log.isWarnEnabled()) {
            StringBuilder sb = new StringBuilder();

            sb.append("session=\"");
            sb.append(session.getSessionId());
            sb.append("\" - ");

            sb.append("ADD dn=\"");
            sb.append(request.getDn());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, AddResponse response) {

        if (log.isWarnEnabled()) {
            StringBuilder sb = new StringBuilder();

            sb.append("session=\"");
            sb.append(session.getSessionId());
            sb.append("\" - ");

            sb.append("ADD result=\"");
            sb.append(response.getMessage());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, BindRequest request) {

        if (log.isWarnEnabled()) {
            StringBuilder sb = new StringBuilder();

            sb.append("session=\"");
            sb.append(session.getSessionId());
            sb.append("\" - ");

            sb.append("BIND dn=\"");
            sb.append(request.getDn());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, BindResponse response) {

        if (log.isWarnEnabled()) {
            StringBuilder sb = new StringBuilder();

            sb.append("session=\"");
            sb.append(session.getSessionId());
            sb.append("\" - ");

            sb.append("BIND result=\"");
            sb.append(response.getMessage());
            sb.append("\"");

            if (response.getReturnCode() == LDAP.SUCCESS && session.getBindDn() != null) {
                sb.append(" authDn=\"");
                sb.append(session.getBindDn());
                sb.append("\"");
            }

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, CompareRequest request) {

        if (log.isWarnEnabled()) {
            StringBuilder sb = new StringBuilder();

            sb.append("session=\"");
            sb.append(session.getSessionId());
            sb.append("\" - ");

            sb.append("COMPARE dn=\"");
            sb.append(request.getDn());
            sb.append("\" attr=\"");
            sb.append(request.getAttributeName());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, CompareResponse response) {

        if (log.isWarnEnabled()) {
            StringBuilder sb = new StringBuilder();

            sb.append("session=\"");
            sb.append(session.getSessionId());
            sb.append("\" - ");

            sb.append("COMPARE result=\"");
            sb.append(response.getMessage());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, DeleteRequest request) {

        if (log.isWarnEnabled()) {
            StringBuilder sb = new StringBuilder();

            sb.append("session=\"");
            sb.append(session.getSessionId());
            sb.append("\" - ");

            sb.append("DELETE dn=\"");
            sb.append(request.getDn());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, DeleteResponse response) {

        if (log.isWarnEnabled()) {
            StringBuilder sb = new StringBuilder();

            sb.append("session=\"");
            sb.append(session.getSessionId());
            sb.append("\" - ");

            sb.append("DELETE result=\"");
            sb.append(response.getMessage());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, ModifyRequest request) {

        if (log.isWarnEnabled()) {
            StringBuilder sb = new StringBuilder();

            sb.append("session=\"");
            sb.append(session.getSessionId());
            sb.append("\" - ");

            sb.append("MODIFY dn=\"");
            sb.append(request.getDn());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, ModifyResponse response) {

        if (log.isWarnEnabled()) {
            StringBuilder sb = new StringBuilder();

            sb.append("session=\"");
            sb.append(session.getSessionId());
            sb.append("\" - ");

            sb.append("MODIFY result=\"");
            sb.append(response.getMessage());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, ModRdnRequest request) {

        if (log.isWarnEnabled()) {
            StringBuilder sb = new StringBuilder();

            sb.append("session=\"");
            sb.append(session.getSessionId());
            sb.append("\" - ");

            sb.append("MODRDN dn=\"");
            sb.append(request.getDn());
            sb.append("\" newRdn=\"");
            sb.append(request.getNewRdn());
            sb.append("\" deleteOldRdn=\"");
            sb.append(request.getDeleteOldRdn());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, ModRdnResponse response) {

        if (log.isWarnEnabled()) {
            StringBuilder sb = new StringBuilder();

            sb.append("session=\"");
            sb.append(session.getSessionId());
            sb.append("\" - ");

            sb.append("MODRDN result=\"");
            sb.append(response.getMessage());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, SearchRequest request) {

        if (log.isWarnEnabled()) {
            StringBuilder sb = new StringBuilder();

            sb.append("session=\"");
            sb.append(session.getSessionId());
            sb.append("\" - ");

            sb.append("SEARCH base=\"");
            sb.append(request.getDn());
            sb.append("\" scope=\"");
            sb.append(LDAP.getScope(request.getScope()));
            sb.append("\" filter=\"");

            Filter filter = request.getFilter();
            sb.append(filter == null ? "(objectClass=*)" : filter.toString());

            sb.append("\" attrs=\"");

            boolean first = true;
            for (String attribute : request.getAttributes()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }
                sb.append(attribute);
            }
            
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, SearchResponse<SearchResult> response) {

        if (log.isWarnEnabled()) {
            StringBuilder sb = new StringBuilder();

            sb.append("session=\"");
            sb.append(session.getSessionId());
            sb.append("\" - ");

            sb.append("SEARCH result=\"");
            sb.append(response.getMessage());
            sb.append("\" entries=\"");

            sb.append(response.getTotalCount());

            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, UnbindRequest request) {

        if (log.isWarnEnabled()) {
            StringBuilder sb = new StringBuilder();

            sb.append("session=\"");
            sb.append(session.getSessionId());
            sb.append("\" - ");

            sb.append("UNBIND");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, UnbindResponse response) {

        if (log.isWarnEnabled()) {
            StringBuilder sb = new StringBuilder();

            sb.append("session=\"");
            sb.append(session.getSessionId());
            sb.append("\" - ");

            sb.append("UNBIND result=\"");
            sb.append(response.getMessage());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

}
