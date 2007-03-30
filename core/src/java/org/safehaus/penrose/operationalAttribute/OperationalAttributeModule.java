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
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.entry.Attribute;
import org.safehaus.penrose.ldap.ModifyRequest;
import org.safehaus.penrose.ldap.AddRequest;
import org.safehaus.penrose.ldap.ModRdnRequest;
import org.safehaus.penrose.ldap.Modification;

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

        Attributes attributes = request.getAttributes();

        if (bindDn != null) {
            attributes.setValue("creatorsName", bindDn.toString());
        }

        attributes.setValue("createTimestamp", timestamp);

        if (bindDn != null) {
            attributes.setValue("modifiersName", bindDn.toString());
        }

        attributes.setValue("modifyTimestamp", timestamp);

        return true;
    }

    public boolean beforeModify(ModifyEvent event) throws Exception {

        Date date = new Date();
        String timestamp = OperationalAttribute.formatDate(date);

        ModifyRequest request = event.getRequest();
        System.out.println("#### Modifying "+request.getDn()+" at "+timestamp);

        Session session = event.getSession();
        DN bindDn = session.getBindDn();

        Collection<Modification> modifications = request.getModifications();

        if (bindDn != null) {
            Attribute modifiersName = new Attribute("modifiersName", bindDn.toString());
            Modification mi = new Modification(Modification.REPLACE, modifiersName);
            modifications.add(mi);
        }

        Attribute modifyTimestamp = new Attribute("modifyTimestamp", timestamp);
        Modification mi = new Modification(Modification.REPLACE, modifyTimestamp);
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

        Collection<Modification> modifications = new ArrayList<Modification>();

        if (bindDn != null) {
            Attribute modifiersName = new Attribute("modifiersName", bindDn.toString());
            Modification mi = new Modification(Modification.REPLACE, modifiersName);
            modifications.add(mi);
        }

        Attribute modifyTimestamp = new Attribute("modifyTimestamp", timestamp);
        Modification mi = new Modification(Modification.REPLACE, modifyTimestamp);
        modifications.add(mi);

        DN dn = request.getDn();
        session.modify(dn, modifications);
    }
}
