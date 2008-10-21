package org.safehaus.penrose.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.LDAP;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.filter.Filter;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class Access {

    public static Logger log = LoggerFactory.getLogger(Access.class);
    public static boolean warn = log.isWarnEnabled();

    public static void log(Session session, ConnectRequest request) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("CONNECT session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            if (request.getClientAddress() != null) {
                sb.append(" from=\"");
                sb.append(request.getClientAddress());
                sb.append("\"");
            }

            if (request.getServerAddress() != null) {
                sb.append(" to=\"");
                sb.append(request.getServerAddress());
                sb.append("\"");
            }

            if (request.getProtocol() != null) {
                sb.append(" protocol=\"");
                sb.append(request.getProtocol());
                sb.append("\"");
            }

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, DisconnectRequest request) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("DISCONNECT session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, AbandonRequest request) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("ABANDON session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" operation=\"");
            sb.append(request.getOperationName());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, AbandonResponse response) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("ABANDON session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = response.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" result=\"");
            sb.append(response.getErrorMessage());
            sb.append("\"");

            log.warn(sb.toString());
        }

    }

    public static void log(Session session, AddRequest request) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("ADD session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" dn=\"");
            sb.append(request.getDn());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, AddResponse response) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("ADD session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = response.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" result=\"");
            sb.append(response.getErrorMessage());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, BindRequest request) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("BIND session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" dn=\"");
            sb.append(request.getDn());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, BindResponse response) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("BIND session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = response.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" result=\"");
            sb.append(response.getErrorMessage());
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

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("COMPARE session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" dn=\"");
            sb.append(request.getDn());
            sb.append("\" attr=\"");
            sb.append(request.getAttributeName());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, CompareResponse response) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("COMPARE session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = response.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" result=\"");
            sb.append(response.getErrorMessage());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, DeleteRequest request) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("DELETE session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" dn=\"");
            sb.append(request.getDn());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, DeleteResponse response) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("DELETE session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = response.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" result=\"");
            sb.append(response.getErrorMessage());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, ModifyRequest request) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("MODIFY session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" dn=\"");
            sb.append(request.getDn());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, ModifyResponse response) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("MODIFY session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = response.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" result=\"");
            sb.append(response.getErrorMessage());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, ModRdnRequest request) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("MODRDN session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" dn=\"");
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

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("MODRDN session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = response.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" result=\"");
            sb.append(response.getErrorMessage());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, SearchRequest request) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("SEARCH session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" base=\"");
            sb.append(request.getDn());
            sb.append("\" scope=\"");
            sb.append(LDAP.getScope(request.getScope()));
            sb.append("\" filter=\"");

            Filter filter = request.getFilter();
            sb.append(filter == null ? "(objectClass=*)" : filter.toString());

            sb.append("\"");

            Collection<String> attributes = request.getAttributes();
            if (attributes.isEmpty()) {
                sb.append(" attrs=\"");

                boolean first = true;
                for (String attribute : attributes) {
                    if (first) {
                        first = false;
                    } else {
                        sb.append(",");
                    }
                    sb.append(attribute);
                }

                sb.append("\"");
            }

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, SearchResponse response) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("SEARCH session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = response.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" result=\"");
            sb.append(response.getErrorMessage());
            sb.append("\" entries=\"");
            sb.append(response.getTotalCount());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, UnbindRequest request) {

        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("UNBIND session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, UnbindResponse response) {
/*
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("UNBIND session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = response.getMessageId();
            if (messageId != null) {
                sb.append(" message=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" result=\"");
            sb.append(response.getErrorMessage());
            sb.append("\"");

            log.warn(sb.toString());
        }
*/
    }
}
