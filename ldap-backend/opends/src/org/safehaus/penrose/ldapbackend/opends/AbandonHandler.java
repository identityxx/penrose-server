package org.safehaus.penrose.ldapbackend.opends;

import org.opends.server.api.plugin.PreParsePluginResult;
import org.opends.server.api.ClientConnection;
import org.opends.server.types.operation.PreParseAbandonOperation;
import org.opends.server.types.ResultCode;
import org.opends.server.types.CancelRequest;
import org.opends.server.core.AbandonOperation;
import org.opends.server.protocols.ldap.LDAPClientConnection;
import org.opends.messages.MessageBuilder;
import org.opends.messages.Message;
import org.safehaus.penrose.ldapbackend.Connection;
import org.safehaus.penrose.ldapbackend.AbandonRequest;
import org.safehaus.penrose.ldapbackend.AbandonResponse;

/**
 * @author Endi Sukma Dewata
 */
public class AbandonHandler extends Handler {

    public AbandonHandler(LDAPBackendPlugin plugin) {
        super(plugin);
    }

    public PreParsePluginResult process(PreParseAbandonOperation operation) {
        AbandonOperation op = (AbandonOperation)operation;

        try {
            int messageId = op.getMessageID();
            int idToAbandon = op.getIDToAbandon();

            if (debug) log("abandon", "abandon(\""+idToAbandon+"\")");

            if (plugin.backend == null) {
                if (debug) log("abandon", "Missing backend.");
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

            ClientConnection clientConnection = operation.getClientConnection();

            if (clientConnection instanceof LDAPClientConnection) {
                final LDAPClientConnection ldapConnection = (LDAPClientConnection)clientConnection;

                Message message = new MessageBuilder("Operation "+idToAbandon+" abandoned.").toMessage();
                CancelRequest cancelRequest = new CancelRequest(true, message);
                ldapConnection.cancelOperation(idToAbandon, cancelRequest);
            }

            AbandonRequest request = plugin.backend.createAbandonRequest();
            request.setMessageId(messageId);
            request.setIdToAbandon(idToAbandon);
            getControls(op, request);

            AbandonResponse response = plugin.backend.createAbandonResponse();
            response.setMessageId(messageId);

            connection.abandon(request, response);

            setControls(response, op);

            int rc = response.getReturnCode();
            if (rc != 0) {
                String errorMessage = response.getErrorMessage();
                if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            }

            op.setResultCode(ResultCode.valueOf(rc));

        } catch (Exception e) {
            if (debug) log("abandon", e);
            String errorMessage = e.getMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.OPERATIONS_ERROR);
        }

        return new PreParsePluginResult(false, false, true);
    }
}
