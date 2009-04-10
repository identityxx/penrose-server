/**
 * Copyright 2009 Red Hat, Inc.
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
package org.safehaus.penrose.ldapbackend.opends;

import org.safehaus.penrose.ldapbackend.Backend;
import org.safehaus.penrose.ldapbackend.Request;
import org.safehaus.penrose.ldapbackend.Response;
import org.opends.server.protocols.asn1.ASN1OctetString;
import org.opends.server.types.Control;
import org.opends.server.types.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * @author Endi S. Dewata
 */
public class Handler {

    public Logger log = LoggerFactory.getLogger(getClass());

    public boolean debug;

    public LDAPBackendPlugin plugin;

    public Handler(LDAPBackendPlugin plugin) {
        this.plugin = plugin;
    }

    public void log(String method, String message) {
        plugin.log(method, message);
    }

    public void log(String method, Throwable exception) {
        plugin.log(method, exception);
    }

    public void getControls(Operation op, Request request) throws Exception {

        List<Control> controls = op.getRequestControls();
        if (controls == null || controls.isEmpty()) return;

        if (debug) log("request", "Controls:");

        for (Control control : controls) {

            String oid = control.getOID();
            ASN1OctetString os = control.getValue();
            byte[] value = os == null ? null : os.value();
            boolean critical = control.isCritical();

            if (debug) log("request", " - " + oid + ": " + critical);

            org.safehaus.penrose.ldapbackend.Control ctrl = plugin.backend.createControl(oid, value, critical);
            request.addControl(ctrl);
        }
    }

    public void setControls(Response response, Operation op) throws Exception {

        Collection<org.safehaus.penrose.ldapbackend.Control> controls = response.getControls();
        if (controls == null || controls.isEmpty()) return;

        if (debug) log("response", "Controls:");

        for (org.safehaus.penrose.ldapbackend.Control control : controls) {
            if (debug) log("response", " - " + control.getOid() + ": " + control.isCritical());

            Control openDsControl = createControl(control);
            op.addResponseControl(openDsControl);
        }
    }

    public Control createControl(org.safehaus.penrose.ldapbackend.Control control) throws Exception {

        String oid = control.getOid();
        boolean critical = control.isCritical();
        byte[] value = control.getValue();
        ASN1OctetString os = value == null ? null : new ASN1OctetString(value);

        if (debug) log("response", " - "+oid+": "+critical);

        return new Control(oid, critical, os);
    }

    public Backend getBackend() {
        return plugin.backend;
    }

    public void setBackend(Backend backend) {
        this.plugin.backend = backend;
    }
}
