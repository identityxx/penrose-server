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
import org.opends.server.types.operation.PreParseBindOperation;
import org.opends.server.types.*;
import org.opends.server.types.Attribute;
import org.opends.server.types.DN;
import org.opends.server.types.RDN;
import org.opends.server.core.BindOperation;
import org.opends.messages.MessageBuilder;
import org.safehaus.penrose.ldapbackend.*;

import java.util.*;

import org.safehaus.penrose.ldapbackend.Connection;

/**
 * @author Endi S. Dewata
 */
public class BindHandler extends Handler {

    public BindHandler(LDAPBackendPlugin plugin) {
        super(plugin);
    }

    public PreParsePluginResult process(PreParseBindOperation operation) {
        BindOperation op = (BindOperation)operation;

        try {
            int messageId = op.getMessageID();
            String rawDn = op.getRawBindDN().toString();
            byte[] password = op.getSimplePassword().value();

            if (debug) log("bind", "bind(\""+rawDn+"\", \""+new String(password)+"\")");

            if (plugin.backend == null) {
                if (debug) log("bind", "Missing backend.");
                return new PreParsePluginResult(false, true, false);
            }

            org.safehaus.penrose.ldapbackend.DN dn = plugin.backend.createDn(rawDn);

            if (!plugin.backend.contains(dn)) {
                if (debug) log("bind", "Bypassing "+dn);
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

            BindRequest request = plugin.backend.createBindRequest();
            request.setMessageId(messageId);
            request.setDn(dn);
            request.setPassword(password);
            getControls(op, request);

            BindResponse response = plugin.backend.createBindResponse();
            response.setMessageId(messageId);

            connection.bind(request, response);

            Entry entry = createEntry(op);
            
            if (entry != null) {
                AuthenticationInfo authInfo = new AuthenticationInfo(
                        entry,
                        op.getSimplePassword(),
                        connection.isRoot()
                );

                op.setAuthenticationInfo(authInfo);
            }

            setControls(response, op);

            int rc = response.getReturnCode();
            if (rc != 0) {
                String errorMessage = response.getErrorMessage();
                if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            }
            op.setResultCode(ResultCode.valueOf(rc));

        } catch (org.ietf.ldap.LDAPException e) {
            if (debug) log("bind", e);
            String errorMessage = e.getLDAPErrorMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.valueOf(e.getResultCode()));

        } catch (Exception e) {
            if (debug) log("bind", e);
            String errorMessage = e.getMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.OPERATIONS_ERROR);
        }

        return new PreParsePluginResult(false, false, true);
    }

    public Entry createEntry(BindOperation op) throws Exception {
        DN dn = DN.decode(op.getRawBindDN());
        if (dn.isNullDN()) return null;

        Map<ObjectClass,String> objectClasses = new HashMap<ObjectClass,String>();
        ObjectClass oc = DirectoryConfig.getObjectClass("person", true);
        objectClasses.put(oc, "person");

        Map<AttributeType, List<Attribute>> userAttributes = new HashMap<AttributeType,List<Attribute>>();

        RDN rdn = dn.getRDN();
        for (int i=0; i<rdn.getNumValues(); i++) {
            String name = rdn.getAttributeName(i);
            AttributeValue value = rdn.getAttributeValue(i);
            AttributeType at = rdn.getAttributeType(i);
            //AttributeType at = DirectoryConfig.getAttributeType(name.toLowerCase(), true);

            LinkedHashSet<AttributeValue> values = new LinkedHashSet<AttributeValue>();
            values.add(value);

            List<Attribute> attributes = new ArrayList<Attribute>();
            attributes.add(new Attribute(at, name, values));

            userAttributes.put(at, attributes);
        }

        Map<AttributeType,List<Attribute>> operationalAttributes = new HashMap<AttributeType,List<Attribute>>();

        return new Entry(
                dn,
                objectClasses,
                userAttributes,
                operationalAttributes
        );
    }
}
