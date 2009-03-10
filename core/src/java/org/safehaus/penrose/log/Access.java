package org.safehaus.penrose.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.LDAP;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.operation.SearchOperation;
import org.safehaus.penrose.filter.Filter;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class Access {

    public static Logger log = LoggerFactory.getLogger(Access.class);

    public static void log(Session session, ConnectRequest request) {

        boolean warn = log.isWarnEnabled();
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

        boolean warn = log.isWarnEnabled();
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("DISCONNECT session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, AbandonRequest request) {

        boolean warn = log.isWarnEnabled();
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("ABANDON session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" operation=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" operationToAbandon=\"");
            sb.append(request.getOperationName());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, AbandonResponse response) {

        boolean warn = log.isWarnEnabled();
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("ABANDON session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = response.getMessageId();
            if (messageId != null) {
                sb.append(" operation=\"");
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

        boolean warn = log.isWarnEnabled();
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("ADD session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" operation=\"");
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

        boolean warn = log.isWarnEnabled();
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("ADD session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = response.getMessageId();
            if (messageId != null) {
                sb.append(" operation=\"");
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

        boolean warn = log.isWarnEnabled();
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("BIND session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" operation=\"");
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

        boolean warn = log.isWarnEnabled();
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("BIND session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = response.getMessageId();
            if (messageId != null) {
                sb.append(" operation=\"");
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

        boolean warn = log.isWarnEnabled();
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("COMPARE session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" operation=\"");
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

        boolean warn = log.isWarnEnabled();
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("COMPARE session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = response.getMessageId();
            if (messageId != null) {
                sb.append(" operation=\"");
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

        boolean warn = log.isWarnEnabled();
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("DELETE session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" operation=\"");
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

        boolean warn = log.isWarnEnabled();
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("DELETE session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = response.getMessageId();
            if (messageId != null) {
                sb.append(" operation=\"");
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

        boolean warn = log.isWarnEnabled();
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("MODIFY session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" operation=\"");
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

        boolean warn = log.isWarnEnabled();
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("MODIFY session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = response.getMessageId();
            if (messageId != null) {
                sb.append(" operation=\"");
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

        boolean warn = log.isWarnEnabled();
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("MODRDN session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" operation=\"");
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

        boolean warn = log.isWarnEnabled();
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("MODRDN session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = response.getMessageId();
            if (messageId != null) {
                sb.append(" operation=\"");
                sb.append(messageId);
                sb.append("\"");
            }

            sb.append(" result=\"");
            sb.append(response.getErrorMessage());
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(SearchOperation operation) {

        boolean warn = log.isWarnEnabled();
        if (warn) {

            StringBuilder sb = new StringBuilder();

            sb.append("SEARCH session=\"");
            sb.append(operation.getSessionName());
            sb.append("\" operation=\"");
            sb.append(operation.getOperationName());
            sb.append("\" base=\"");
            sb.append(operation.getDn());
            sb.append("\" scope=\"");
            sb.append(LDAP.getScope(operation.getScope()));
            sb.append("\" filter=\"");

            Filter filter = operation.getFilter();
            sb.append(filter == null ? "(objectClass=*)" : filter.toString());

            sb.append("\"");

            Collection<String> attributes = operation.getAttributes();
            if (!attributes.isEmpty()) {
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

    public static void log(SearchOperation searchOperation, long elapsedTime) {

        boolean warn = log.isWarnEnabled();
        if (warn) {

            SearchResponse response = searchOperation.getSearchResponse();

            StringBuilder sb = new StringBuilder();

            sb.append("SEARCH session=\"");
            sb.append(searchOperation.getSessionName());
            sb.append("\" operation=\"");
            sb.append(searchOperation.getOperationName());
            sb.append("\" result=\"");
            sb.append(response.getErrorMessage());
            sb.append("\" entries=\"");
            sb.append(response.getTotalCount());
            sb.append("\" time=\"");
            sb.append(elapsedTime);
            sb.append("\"");

            log.warn(sb.toString());
        }
    }

    public static void log(Session session, UnbindRequest request) {

        boolean warn = log.isWarnEnabled();
        if (warn) {
            StringBuilder sb = new StringBuilder();

            sb.append("UNBIND session=\"");
            sb.append(session.getSessionName());
            sb.append("\"");

            Integer messageId = request.getMessageId();
            if (messageId != null) {
                sb.append(" operation=\"");
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
                sb.append(" operation=\"");
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
