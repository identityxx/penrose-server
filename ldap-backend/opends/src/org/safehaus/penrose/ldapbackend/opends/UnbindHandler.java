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
import org.opends.server.types.operation.PreParseUnbindOperation;
import org.opends.server.types.ResultCode;
import org.opends.server.core.UnbindOperation;
import org.opends.messages.MessageBuilder;
import org.safehaus.penrose.ldapbackend.UnbindResponse;
import org.safehaus.penrose.ldapbackend.Connection;
import org.safehaus.penrose.ldapbackend.DN;
import org.safehaus.penrose.ldapbackend.UnbindRequest;

/**
 * @author Endi S. Dewata
 */
public class UnbindHandler extends Handler {

    public UnbindHandler(LDAPBackendPlugin plugin) {
        super(plugin);
    }

    public PreParsePluginResult process(
            PreParseUnbindOperation unbindOperation
    ) {
        UnbindOperation op = (UnbindOperation)unbindOperation;

        try {
            int messageId = op.getMessageID();
            String rawDn = op.getAuthorizationDN().toString();

            if (debug) log("unbind", "unbind(\""+rawDn+"\")");

            if (plugin.backend == null) {
                if (debug) log("unbind", "Missing backend.");
                return new PreParsePluginResult(false, true, false);
            }

            DN dn = plugin.backend.createDn(rawDn);

            if (!plugin.backend.contains(dn)) {
                if (debug) log("unbind", "Bypassing "+dn);
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

            UnbindRequest request = plugin.backend.createUnbindRequest();
            request.setMessageId(messageId);
            getControls(op, request);

            UnbindResponse response = plugin.backend.createUnbindResponse();
            response.setMessageId(messageId);

            connection.unbind(request, response);

            setControls(response, op);

            int rc = response.getReturnCode();
            if (rc != 0) {
                String errorMessage = response.getErrorMessage();
                if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            }
            op.setResultCode(ResultCode.valueOf(rc));

        } catch (org.ietf.ldap.LDAPException e) {
            if (debug) log("unbind", e);
            String errorMessage = e.getLDAPErrorMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.valueOf(e.getResultCode()));

        } catch (Exception e) {
            if (debug) log("unbind", e);
            String errorMessage = e.getMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.OPERATIONS_ERROR);
        }

        return new PreParsePluginResult(false, false, true);
    }
}
