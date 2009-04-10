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
import org.opends.server.types.operation.PreParseModifyDNOperation;
import org.opends.server.types.ResultCode;
import org.opends.server.core.ModifyDNOperation;
import org.opends.messages.MessageBuilder;
import org.safehaus.penrose.ldapbackend.Connection;
import org.safehaus.penrose.ldapbackend.*;

/**
 * @author Endi S. Dewata
 */
public class ModifyDNHandler extends Handler {

    public ModifyDNHandler(LDAPBackendPlugin plugin) {
        super(plugin);
    }

    public PreParsePluginResult process(PreParseModifyDNOperation operation) {
        ModifyDNOperation op = (ModifyDNOperation)operation;

        try {
            int messageId = op.getMessageID();
            String rawDn = op.getRawEntryDN().toString();
            String rawNewRdn = op.getRawNewRDN().toString();
            boolean deleteOldRdn = op.deleteOldRDN();

            if (debug) log("modifydn", "modifydn(\""+rawDn+"\")");

            if (plugin.backend == null) {
                if (debug) log("modifydn", "Bypassing "+rawDn);
                return new PreParsePluginResult(false, true, false);
            }

            DN dn = plugin.backend.createDn(rawDn);
            RDN newRdn = plugin.backend.createRdn(rawNewRdn);

            if (!plugin.backend.contains(dn)) {
                if (debug) log("modifydn", "Bypassing "+dn);
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

            ModRdnRequest request = plugin.backend.createModRdnRequest();
            request.setMessageId(messageId);
            request.setDn(dn);
            request.setNewRdn(newRdn);
            request.setDeleteOldRdn(deleteOldRdn);
            getControls(op, request);

            ModRdnResponse response = plugin.backend.createModRdnResponse();
            response.setMessageId(messageId);

            connection.modrdn(request, response);

            setControls(response, op);

            int rc = response.getReturnCode();
            if (rc != 0) {
                String errorMessage = response.getErrorMessage();
                if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            }
            op.setResultCode(ResultCode.valueOf(rc));

        } catch (org.ietf.ldap.LDAPException e) {
            if (debug) log("modifydn", e);
            String errorMessage = e.getLDAPErrorMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.valueOf(e.getResultCode()));

        } catch (Exception e) {
            if (debug) log("modifydn", e);
            String errorMessage = e.getMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.OPERATIONS_ERROR);
        }

        return new PreParsePluginResult(false, false, true);
    }
}
