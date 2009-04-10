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
import org.opends.server.types.operation.PreParseDeleteOperation;
import org.opends.server.types.ResultCode;
import org.opends.server.core.DeleteOperation;
import org.opends.messages.MessageBuilder;
import org.safehaus.penrose.ldapbackend.DeleteRequest;
import org.safehaus.penrose.ldapbackend.Connection;
import org.safehaus.penrose.ldapbackend.DeleteResponse;
import org.safehaus.penrose.ldapbackend.DN;

/**
 * @author Endi S. Dewata
 */
public class DeleteHandler extends Handler {

    public DeleteHandler(LDAPBackendPlugin plugin) {
        super(plugin);
    }

    public PreParsePluginResult process(PreParseDeleteOperation operation) {
        DeleteOperation op = (DeleteOperation)operation;

        try {
            int messageId = op.getMessageID();
            String rawDn = op.getRawEntryDN().toString();

            if (debug) log("delete", "delete(\""+rawDn+"\")");

            if (plugin.backend == null) {
                if (debug) log("delete", "Missing backend.");
                return new PreParsePluginResult(false, true, false);
            }

            DN dn = plugin.backend.createDn(rawDn);

            if (!plugin.backend.contains(dn)) {
                if (debug) log("delete", "Bypassing "+dn);
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

            DeleteRequest request = plugin.backend.createDeleteRequest();
            request.setMessageId(messageId);
            request.setDn(dn);
            getControls(op, request);

            DeleteResponse response = plugin.backend.createDeleteResponse();
            response.setMessageId(messageId);

            connection.delete(request, response);

            setControls(response, op);

            int rc = response.getReturnCode();
            if (rc != 0) {
                String errorMessage = response.getErrorMessage();
                if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            }
            op.setResultCode(ResultCode.valueOf(rc));

        } catch (org.ietf.ldap.LDAPException e) {
            if (debug) log("delete", e);
            String errorMessage = e.getLDAPErrorMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.valueOf(e.getResultCode()));

        } catch (Exception e) {
            if (debug) log("delete", e);
            String errorMessage = e.getMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.OPERATIONS_ERROR);
        }

        return new PreParsePluginResult(false, false, true);
    }
}
