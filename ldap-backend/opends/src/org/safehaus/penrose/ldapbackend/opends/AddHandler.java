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
import org.opends.server.types.operation.PreParseAddOperation;
import org.opends.server.types.ResultCode;
import org.opends.server.types.RawAttribute;
import org.opends.server.core.AddOperation;
import org.opends.server.protocols.ldap.LDAPAttribute;
import org.opends.server.protocols.asn1.ASN1OctetString;
import org.opends.messages.MessageBuilder;

import java.util.List;

import org.safehaus.penrose.ldapbackend.*;
import org.safehaus.penrose.ldapbackend.Connection;

/**
 * @author Endi S. Dewata
 */
public class AddHandler extends Handler {

    public AddHandler(LDAPBackendPlugin plugin) {
        super(plugin);
    }

    public PreParsePluginResult process(PreParseAddOperation operation) {
        AddOperation op = (AddOperation)operation;

        try {
            int messageId = op.getMessageID();
            String rawDn = op.getRawEntryDN().toString();
            List<RawAttribute> rawAttributes = op.getRawAttributes();

            if (debug) log("add", "add(\""+rawDn+"\")");

            if (plugin.backend == null) {
                if (debug) log("add", "Missing backend.");
                return new PreParsePluginResult(false, true, false);
            }

            DN dn = plugin.backend.createDn(rawDn);
            Attributes attributes = createAttributes(rawAttributes);

            if (!plugin.backend.contains(dn)) {
                if (debug) log("add", "Bypassing "+dn);
                return new PreParsePluginResult(false, true, false);
                //return PreParsePluginResult.SUCCESS;
            }

            long connectionId = op.getConnectionID();
            Connection connection = plugin.getConnection(connectionId);

            if (connection == null) {
                if (debug) log("search", "Invalid connection "+connectionId+".");
                op.setErrorMessage(new MessageBuilder("Invalid connection "+connectionId+"."));
                op.setResultCode(ResultCode.OPERATIONS_ERROR);
                return new PreParsePluginResult(false, false, true);
            }

            AddRequest request = plugin.backend.createAddRequest();
            request.setMessageId(messageId);
            request.setDn(dn);
            request.setAttributes(attributes);
            getControls(op, request);

            AddResponse response = plugin.backend.createAddResponse();
            response.setMessageId(messageId);

            connection.add(request, response);

            //op.setEntryToAdd(plugin.createEntry(dn, attributes));

            setControls(response, op);

            int rc = response.getReturnCode();
            if (rc != 0) {
                String errorMessage = response.getErrorMessage();
                if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            }
            op.setResultCode(ResultCode.valueOf(rc));

        } catch (org.ietf.ldap.LDAPException e) {
            if (debug) log("add", e);
            String errorMessage = e.getLDAPErrorMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.valueOf(e.getResultCode()));

        } catch (Exception e) {
            if (debug) log("add", e);
            String errorMessage = e.getMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.OPERATIONS_ERROR);
        }

        return new PreParsePluginResult(false, false, true);
    }

    public Attributes createAttributes(List<RawAttribute> ldapAttributes) throws Exception {
        Attributes attributes = plugin.backend.createAttributes();

        for (RawAttribute rawAttribute : ldapAttributes) {
            LDAPAttribute ldapAttribute = (LDAPAttribute) rawAttribute;
            String name = ldapAttribute.getAttributeType();

            Attribute attribute = plugin.backend.createAttribute(name);
            for (ASN1OctetString s : ldapAttribute.getValues()) {
                attribute.addValue(s.toString());
            }
            attributes.add(attribute);
        }

        return attributes;
    }
}
