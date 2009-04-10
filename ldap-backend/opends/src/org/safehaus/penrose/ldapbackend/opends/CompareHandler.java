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
import org.opends.server.types.operation.PreParseCompareOperation;
import org.opends.server.types.ResultCode;
import org.opends.server.core.CompareOperation;
import org.opends.messages.MessageBuilder;
import org.safehaus.penrose.ldapbackend.CompareRequest;
import org.safehaus.penrose.ldapbackend.DN;
import org.safehaus.penrose.ldapbackend.CompareResponse;
import org.safehaus.penrose.ldapbackend.Connection;

/**
 * @author Endi S. Dewata
 */
public class CompareHandler extends Handler {

    public CompareHandler(LDAPBackendPlugin plugin) {
        super(plugin);
    }

    public PreParsePluginResult process(PreParseCompareOperation operation) {
        CompareOperation op = (CompareOperation)operation;

        try {
            int messageId = op.getMessageID();
            String rawDn = op.getRawEntryDN().toString();
            String type = op.getRawAttributeType();
            String value = op.getAssertionValue().toString();

            if (debug) log("compare", "compare(\""+rawDn+"\")");

            if (plugin.backend == null) {
                if (debug) log("compare", "Missing backend.");
                return new PreParsePluginResult(false, true, false);
            }

            DN dn = plugin.backend.createDn(rawDn);

            if (!plugin.backend.contains(dn)) {
                if (debug) log("compare", "Bypassing "+dn);
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

            CompareRequest request = plugin.backend.createCompareRequest();
            request.setMessageId(messageId);
            request.setDn(dn);
            request.setAttributeName(type);
            request.setAttributeValue(value);
            getControls(op, request);

            CompareResponse response = plugin.backend.createCompareResponse();
            response.setMessageId(messageId);

            connection.compare(request, response);

            setControls(response, op);

            int rc = response.getReturnCode();
            if (rc != 0) {
                String errorMessage = response.getErrorMessage();
                if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            }
            op.setResultCode(ResultCode.valueOf(rc));

        } catch (org.ietf.ldap.LDAPException e) {
            if (debug) log("compare", e);
            String errorMessage = e.getLDAPErrorMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.valueOf(e.getResultCode()));

        } catch (Exception e) {
            if (debug) log("compare", e);
            String errorMessage = e.getMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.OPERATIONS_ERROR);
        }

        return new PreParsePluginResult(false, false, true);
    }
}
