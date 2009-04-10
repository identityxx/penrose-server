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

import org.opends.server.api.plugin.PreParsePluginResult;
import org.opends.server.types.operation.PreParseModifyOperation;
import org.opends.server.types.RawModification;
import org.opends.server.types.ResultCode;
import org.opends.server.core.ModifyOperation;
import org.opends.server.protocols.ldap.LDAPModification;
import org.opends.server.protocols.ldap.LDAPAttribute;
import org.opends.server.protocols.asn1.ASN1OctetString;
import org.opends.messages.MessageBuilder;
import org.safehaus.penrose.ldapbackend.*;
import org.safehaus.penrose.ldapbackend.Connection;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class ModifyHandler extends Handler {

    public ModifyHandler(LDAPBackendPlugin plugin) {
        super(plugin);
    }

    public PreParsePluginResult process(PreParseModifyOperation operation) {
        ModifyOperation op = (ModifyOperation)operation;

        try {
            int messageId = op.getMessageID();
            String rawDn = op.getRawEntryDN().toString();

            if (debug) log("modify", "modify(\""+rawDn+"\")");

            if (plugin.backend == null) {
                if (debug) log("modify", "Missing backend.");
                return new PreParsePluginResult(false, true, false);
            }

            DN dn = plugin.backend.createDn(rawDn);

            if (!plugin.backend.contains(dn)) {
                if (debug) log("modify", "Bypassing "+dn);
                //return PreParsePluginResult.SUCCESS;
                return new PreParsePluginResult(false, true, false);
            }

            long connectionId = op.getConnectionID();
            Connection connection = plugin.getConnection(connectionId);

            if (connection == null) {
                if (debug) log("search", "Invalid connection "+connectionId+".");
                op.setErrorMessage(new MessageBuilder("Invalid connection "+connectionId+"."));
                op.setResultCode(ResultCode.OPERATIONS_ERROR);
                return new PreParsePluginResult(false, false, true);
            }

            Collection<Modification> modifications = new ArrayList<Modification>();
            for (RawModification rawModification : op.getRawModifications()) {
                LDAPModification mod = (LDAPModification) rawModification;

                int type;
                switch (mod.getModificationType()) {
                    case ADD:
                        type = Modification.ADD;
                        break;
                    case DELETE:
                        type = Modification.DELETE;
                        break;
                    default:
                        type = Modification.REPLACE;
                        break;
                }

                LDAPAttribute attr = (LDAPAttribute) mod.getAttribute();
                Attribute attribute = plugin.backend.createAttribute(attr.getAttributeType());
                for (ASN1OctetString value : attr.getValues()) {
                    attribute.addValue(value.toString());
                }

                Modification modification = plugin.backend.createModification(type, attribute);
                modifications.add(modification);
            }

            ModifyRequest request = plugin.backend.createModifyRequest();
            request.setMessageId(messageId);
            request.setDn(dn);
            request.setModifications(modifications);
            getControls(op, request);

            ModifyResponse response = plugin.backend.createModifyResponse();
            response.setMessageId(messageId);

            connection.modify(request, response);

            setControls(response, op);

            int rc = response.getReturnCode();
            if (rc != 0) {
                String errorMessage = response.getErrorMessage();
                if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            }
            op.setResultCode(ResultCode.valueOf(rc));

        } catch (org.ietf.ldap.LDAPException e) {
            if (debug) log("modify", e);
            String errorMessage = e.getLDAPErrorMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.valueOf(e.getResultCode()));

        } catch (Exception e) {
            if (debug) log("modify", e);
            String errorMessage = e.getMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.OPERATIONS_ERROR);
        }

        return new PreParsePluginResult(false, false, true);
    }
}
