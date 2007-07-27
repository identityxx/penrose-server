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
package org.safehaus.penrose.module;

import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.event.BindEvent;
import org.safehaus.penrose.ldap.*;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class LockoutModule extends Module {

    public Source source;

    public void init() throws Exception {
        source = partition.getSource("locks");
    }

    public void beforeBind(BindEvent event) throws Exception {

        BindRequest request = event.getRequest();
        BindResponse response = event.getResponse();

        String dn = request.getDn().getNormalizedDn();

        log.debug("Attempting to bind as "+dn+".");

        int counter = 0;

        RDNBuilder rb = new RDNBuilder();
        rb.set("dn", dn);
        RDN rdn = rb.toRdn();

        SearchResult result = source.find(rdn);

        if (result != null) {
            Attributes attributes = result.getAttributes();
            counter = (Integer)attributes.getValue("counter");
        }

        log.debug("Counter: "+counter);

        if (counter >= 3) {
            log.debug("Account has been locked.");

            response.setException(LDAP.createException(LDAP.INVALID_CREDENTIALS));
        }
    }

    public void afterBind(BindEvent event) throws Exception {

        BindRequest request = event.getRequest();
        BindResponse response = event.getResponse();

        String dn = request.getDn().getNormalizedDn();

        if (response.getReturnCode() == LDAP.SUCCESS) {
            log.debug("Bind as "+dn+" succeeded.");
            return;
        }

        log.debug("Bind as "+dn+" failed.");

        RDNBuilder rb = new RDNBuilder();
        rb.set("dn", dn);
        RDN rdn = rb.toRdn();

        SearchResult result = source.find(rdn);

        if (result == null) {
            Attributes attributes = new Attributes();
            attributes.add(new Attribute("dn", dn));
            attributes.add(new Attribute("counter", 1));

            source.add(rdn, attributes);

        } else {
            Attributes attributes = result.getAttributes();
            int counter = (Integer)attributes.getValue("counter");

            counter++;

            Attribute attribute = new Attribute("counter", counter);

            Collection<Modification> modifications = new ArrayList<Modification>();
            modifications.add(new Modification(Modification.REPLACE, attribute));

            source.modify(rdn, modifications);
        }
    }
}
