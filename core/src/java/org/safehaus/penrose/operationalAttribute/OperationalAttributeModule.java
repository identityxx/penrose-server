package org.safehaus.penrose.operationalAttribute;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.event.*;
import org.safehaus.penrose.session.PenroseSession;

import javax.naming.directory.*;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Date;

/**
 * @author Endi S. Dewata
 */
public class OperationalAttributeModule extends Module {

    public void init() throws Exception {
        System.out.println("#### Initializing OperationalAttributeModule.");
    }

    public boolean beforeAdd(AddEvent event) throws Exception {

        Date date = new Date();
        String timestamp = OperationalAttribute.formatDate(date);

        System.out.println("#### Adding "+event.getDn()+" at "+timestamp);

        PenroseSession session = event.getSession();
        String bindDn = session.getBindDn();

        Attributes attributes = event.getAttributes();

        Attribute creatorsName = new BasicAttribute("creatorsName", bindDn);
        attributes.put(creatorsName);

        Attribute createTimestamp = new BasicAttribute("createTimestamp", timestamp);
        attributes.put(createTimestamp);

        Attribute modifiersName = new BasicAttribute("modifiersName", bindDn);
        attributes.put(modifiersName);

        Attribute modifyTimestamp = new BasicAttribute("modifyTimestamp", timestamp);
        attributes.put(modifyTimestamp);

        return true;
    }

    public boolean beforeModify(ModifyEvent event) throws Exception {

        Date date = new Date();
        String timestamp = OperationalAttribute.formatDate(date);

        System.out.println("#### Modifying "+event.getDn()+" at "+timestamp);

        PenroseSession session = event.getSession();
        String bindDn = session.getBindDn();

        Collection modifications = event.getModifications();

        Attribute modifiersName = new BasicAttribute("modifiersName", bindDn);
        ModificationItem mi = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, modifiersName);
        modifications.add(mi);

        Attribute modifyTimestamp = new BasicAttribute("modifyTimestamp", timestamp);
        mi = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, modifyTimestamp);
        modifications.add(mi);

        return true;
    }

    public void afterModRdn(ModRdnEvent event) throws Exception {

        Date date = new Date();
        String timestamp = OperationalAttribute.formatDate(date);

        System.out.println("#### Renaming "+event.getDn()+" at "+timestamp);

        PenroseSession session = event.getSession();
        String bindDn = session.getBindDn();

        Collection modifications = new ArrayList();

        Attribute modifiersName = new BasicAttribute("modifiersName", bindDn);
        ModificationItem mi = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, modifiersName);
        modifications.add(mi);

        Attribute modifyTimestamp = new BasicAttribute("modifyTimestamp", timestamp);
        mi = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, modifyTimestamp);
        modifications.add(mi);

        String dn = event.getDn();
        session.modify(dn, modifications);
    }
}
