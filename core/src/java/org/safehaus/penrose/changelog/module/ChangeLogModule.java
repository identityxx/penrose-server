package org.safehaus.penrose.changelog.module;

import org.safehaus.penrose.event.*;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SessionContext;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceManager;

/**
 * @author Endi Sukma Dewata
 */
public class ChangeLogModule extends Module {

    String sourceName;
    Source changelog;

    public void init() throws Exception {

        sourceName = partition.getParameter("source");
        if (sourceName == null) sourceName = "changelog";

        SourceManager sourceManager = partition.getSourceManager();
        changelog = sourceManager.getSource(sourceName);

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();
        SessionContext sessionContext = penroseContext.getSessionContext();

        EventManager eventManager = sessionContext.getEventManager();
        eventManager.addAddListener(this);
        eventManager.addModifyListener(this);
        eventManager.addModRdnListener(this);
        eventManager.addDeleteListener(this);
    }

    public void afterAdd(AddEvent event) throws Exception {
        AddRequest request = event.getRequest();
        AddResponse response = event.getResponse();

        if (response.getReturnCode() != LDAP.SUCCESS) return;

        recordAddOperation(request);
    }

    public void afterModify(ModifyEvent event) throws Exception {
        ModifyRequest request = event.getRequest();
        ModifyResponse response = event.getResponse();

        if (response.getReturnCode() != LDAP.SUCCESS) return;

        recordModifyOperation(request);
    }

    public void afterModRdn(ModRdnEvent event) throws Exception {
        ModRdnRequest request = event.getRequest();
        ModRdnResponse response = event.getResponse();

        if (response.getReturnCode() != LDAP.SUCCESS) return;

        recordModRdnOperation(request);
    }

    public void afterDelete(DeleteEvent event) throws Exception {
        DeleteRequest request = event.getRequest();
        DeleteResponse response = event.getResponse();

        if (response.getReturnCode() != LDAP.SUCCESS) return;

        recordDeleteOperation(request);
    }

    public void recordAddOperation(AddRequest request) throws Exception {

        log.debug("Recording add operation "+request.getDn());

        Session session = getSession();

        try {
            Attributes attrs = request.getAttributes();

            DN dn = new DN();

            Attributes attributes = new Attributes();
            attributes.setValue("targetDN", request.getDn().toString());
            attributes.setValue("changeType", "add");
            attributes.setValue("changes", attrs.toString());

            changelog.add(session, dn, attributes);

        } finally {
            session.close();
        }
    }

    public void recordModifyOperation(ModifyRequest request) throws Exception {

        log.debug("Recording modify operation "+request.getDn());

        Session session = getSession();

        try {
            StringBuilder sb = new StringBuilder();

            for (Modification modification : request.getModifications()) {
                Attribute attr = modification.getAttribute();

                sb.append(LDAP.getModificationOperation(modification.getType()));
                sb.append(": ");
                sb.append(attr.getName());
                sb.append("\n");

                sb.append(attr);

                sb.append("-");
                sb.append("\n");
            }

            DN dn = new DN();

            Attributes attributes = new Attributes();
            attributes.setValue("targetDN", request.getDn().toString());
            attributes.setValue("changeType", "modify");
            attributes.setValue("changes", sb.toString());

            changelog.add(session, dn, attributes);

        } finally {
            session.close();
        }
    }

    public void recordModRdnOperation(ModRdnRequest request) throws Exception {

        log.debug("Recording modrdn operation "+request.getDn());

        Session session = getSession();

        try {
            DN dn = new DN();

            Attributes attributes = new Attributes();
            attributes.setValue("targetDN", request.getDn().toString());
            attributes.setValue("changeType", "modrdn");
            attributes.setValue("newRDN", request.getNewRdn().toString());
            attributes.setValue("deleteOldRDN", request.getDeleteOldRdn());

            changelog.add(session, dn, attributes);

        } finally {
            session.close();
        }
    }

    public void recordDeleteOperation(DeleteRequest request) throws Exception {

        log.debug("Recording delete operation "+request.getDn());

        Session session = getSession();

        try {
            DN dn = new DN();

            Attributes attributes = new Attributes();
            attributes.setValue("targetDN", request.getDn().toString());
            attributes.setValue("changeType", "delete");

            changelog.add(session, dn, attributes);

        } finally {
            session.close();
        }
    }
}
