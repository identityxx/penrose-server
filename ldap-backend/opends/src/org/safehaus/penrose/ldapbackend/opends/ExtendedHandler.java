package org.safehaus.penrose.ldapbackend.opends;

import org.opends.server.api.plugin.PreParsePluginResult;
import org.opends.server.types.operation.PreParseExtendedOperation;
import org.opends.server.types.ResultCode;
import org.opends.server.core.ExtendedOperation;
import org.opends.messages.MessageBuilder;

/**
 * @author Endi Sukma Dewata
 */
public class ExtendedHandler extends Handler {

    public ExtendedHandler(LDAPBackendPlugin plugin) {
        super(plugin);
    }

    public PreParsePluginResult process(PreParseExtendedOperation operation) {
        ExtendedOperation op = (ExtendedOperation)operation;

        try {
            int messageId = op.getMessageID();

            if (debug) log("extended", "extended()");

            if (plugin.backend == null) {
                if (debug) log("extended", "Missing backend.");
                return new PreParsePluginResult(false, true, false);
            }
/*
        } catch (org.ietf.ldap.LDAPException e) {
            if (debug) log("extended", e);
            String errorMessage = e.getLDAPErrorMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.valueOf(e.getResultCode()));
*/
        } catch (Exception e) {
            if (debug) log("extended", e);
            String errorMessage = e.getMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.OPERATIONS_ERROR);
        }

        return new PreParsePluginResult(false, false, true);
    }
}