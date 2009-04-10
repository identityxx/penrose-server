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
package org.safehaus.penrose.ldapbackend.mina;

import org.apache.mina.handler.demux.MessageHandler;
import org.apache.mina.common.IoSession;
import org.apache.directory.shared.ldap.message.AddRequest;
import org.apache.directory.shared.ldap.message.LdapResult;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.message.AddResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldapbackend.Connection;
import org.safehaus.penrose.ldapbackend.Attributes;
import org.safehaus.penrose.ldapbackend.DN;

/**
 * @author Endi S. Dewata
 */
public class AddHandler implements MessageHandler {

    public Logger log = LoggerFactory.getLogger(getClass());

    MinaHandler handler;

    public AddHandler(MinaHandler handler) {
        this.handler = handler;
    }

    public void messageReceived(IoSession ioSession, Object message) throws Exception {

        AddRequest request = (AddRequest)message;
        AddResponse response = (AddResponse)request.getResultResponse();
        LdapResult result = response.getLdapResult();

        try {
            int messageId = request.getMessageId();
            DN dn = handler.backend.createDn(request.getEntry().toString());
            Attributes attributes = handler.createAttributes(request.getAttributes());

            Long connectionId = handler.getConnectionId(ioSession);
            Connection connection = handler.getConnection(connectionId);

            if (connection == null) {
                log.error("Invalid connection "+connectionId+".");
                return;
            }
            
            org.safehaus.penrose.ldapbackend.AddRequest addRequest = handler.backend.createAddRequest();
            addRequest.setMessageId(messageId);
            addRequest.setDn(dn);
            addRequest.setAttributes(attributes);
            handler.getControls(request, addRequest);

            org.safehaus.penrose.ldapbackend.AddResponse addResponse = handler.backend.createAddResponse();

            connection.add(addRequest, addResponse);

            handler.setControls(addResponse, response);

            int rc = addResponse.getReturnCode();
            if (rc != 0) {
                result.setErrorMessage(addResponse.getErrorMessage());
            }
            result.setResultCode(ResultCodeEnum.getResultCodeEnum(rc));

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            result.setResultCode(ResultCodeEnum.getResultCode(e));
            result.setErrorMessage(e.getMessage());

        } finally {
            ioSession.write(request.getResultResponse());
        }
    }
}
