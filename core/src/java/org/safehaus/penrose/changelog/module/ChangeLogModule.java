package org.safehaus.penrose.changelog.module;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleChain;
import org.safehaus.penrose.session.Session;
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
    }

    public void add(
            Session session,
            AddRequest request,
            AddResponse response,
            ModuleChain chain
    ) throws Exception {

        chain.add(session, request, response);

        if (response.getReturnCode() != LDAP.SUCCESS) return;

        recordAddOperation(request);
    }

    public void delete(
            Session session,
            DeleteRequest request,
            DeleteResponse response,
            ModuleChain chain
    ) throws Exception {

        chain.delete(session, request, response);

        if (response.getReturnCode() != LDAP.SUCCESS) return;

        recordDeleteOperation(request);
    }

    public void modify(
            Session session,
            ModifyRequest request,
            ModifyResponse response,
            ModuleChain chain
    ) throws Exception {

        chain.modify(session, request, response);

        if (response.getReturnCode() != LDAP.SUCCESS) return;

        recordModifyOperation(request);
    }

    public void modrdn(
            Session session,
            ModRdnRequest request,
            ModRdnResponse response,
            ModuleChain chain
    ) throws Exception {

        chain.modrdn(session, request, response);

        if (response.getReturnCode() != LDAP.SUCCESS) return;

        recordModRdnOperation(request);
    }

    public void recordAddOperation(AddRequest request) throws Exception {

        log.debug("Recording add operation "+request.getDn());

        Session session = createAdminSession();

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

        Session session = createAdminSession();

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

        Session session = createAdminSession();

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

        Session session = createAdminSession();

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

    public void clear() throws Exception {
        Session session = createAdminSession();

        try {
            changelog.clear(session);

        } finally {
            session.close();
        }
    }
}
