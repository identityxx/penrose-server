/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.operationalAttribute;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.event.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.AddRequest;
import org.safehaus.penrose.session.ModRdnRequest;
import org.safehaus.penrose.session.ModifyRequest;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.AttributeValues;

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

        AddRequest request = event.getRequest();
        System.out.println("#### Adding "+request.getDn()+" at "+timestamp);

        Session session = event.getSession();
        DN bindDn = session.getBindDn();

        AttributeValues attributeValues = request.getAttributeValues();

        if (bindDn != null) {
            attributeValues.set("creatorsName", bindDn.toString());
        }

        attributeValues.set("createTimestamp", timestamp);

        if (bindDn != null) {
            attributeValues.set("modifiersName", bindDn.toString());
        }

        attributeValues.set("modifyTimestamp", timestamp);

        return true;
    }

    public boolean beforeModify(ModifyEvent event) throws Exception {

        Date date = new Date();
        String timestamp = OperationalAttribute.formatDate(date);

        ModifyRequest request = event.getRequest();
        System.out.println("#### Modifying "+request.getDn()+" at "+timestamp);

        Session session = event.getSession();
        DN bindDn = session.getBindDn();

        Collection modifications = request.getModifications();

        if (bindDn != null) {
            Attribute modifiersName = new BasicAttribute("modifiersName", bindDn.toString());
            ModificationItem mi = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, modifiersName);
            modifications.add(mi);
        }

        Attribute modifyTimestamp = new BasicAttribute("modifyTimestamp", timestamp);
        ModificationItem mi = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, modifyTimestamp);
        modifications.add(mi);

        return true;
    }

    public void afterModRdn(ModRdnEvent event) throws Exception {

        Date date = new Date();
        String timestamp = OperationalAttribute.formatDate(date);

        ModRdnRequest request = event.getRequest();
        System.out.println("#### Renaming "+request.getDn()+" at "+timestamp);

        Session session = event.getSession();
        DN bindDn = session.getBindDn();

        Collection modifications = new ArrayList();

        if (bindDn != null) {
            Attribute modifiersName = new BasicAttribute("modifiersName", bindDn.toString());
            ModificationItem mi = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, modifiersName);
            modifications.add(mi);
        }

        Attribute modifyTimestamp = new BasicAttribute("modifyTimestamp", timestamp);
        ModificationItem mi = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, modifyTimestamp);
        modifications.add(mi);

        DN dn = request.getDn();
        session.modify(dn, modifications);
    }
}
