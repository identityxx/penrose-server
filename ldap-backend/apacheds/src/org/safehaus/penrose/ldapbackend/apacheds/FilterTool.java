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
package org.safehaus.penrose.ldapbackend.apacheds;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.directory.shared.ldap.filter.*;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class FilterTool {

    public static Logger log = LoggerFactory.getLogger(FilterTool.class);

    public static String convert(ExprNode node) throws Exception {
        StringBuilder sb = new StringBuilder();
        FilterTool.convert(node, sb);
        return sb.toString();
    }

    public static void convert(ExprNode node, StringBuilder sb) throws Exception {
        FilterTool.log.debug("Converting filter: "+node);

        if (node == null) return;

        FilterTool.log.debug("Class: "+node.getClass().getName());

        if (node instanceof SimpleNode) {
            SimpleNode simpleNode = (SimpleNode)node;

            String attribute = simpleNode.getAttribute();
            String operation = AbstractExprNode.getOperationString(simpleNode.getAssertionType());
            Object value = simpleNode.getValue();

            sb.append("(");
            sb.append(attribute);
            sb.append(operation);
            sb.append(value);
            sb.append(")");

        } else if (node instanceof PresenceNode) {
            PresenceNode presenceNode = (PresenceNode)node;

            String attribute = presenceNode.getAttribute();
            sb.append("(");
            sb.append(attribute);
            sb.append("=*)");

        } else if (node instanceof BranchNode) {
            BranchNode branchNode = (BranchNode)node;

            if (branchNode.isNegation()) {
                ExprNode childNode = branchNode.getChild();

                sb.append("(!");
                FilterTool.convert(childNode, sb);
                sb.append(")");

            } else if (branchNode.isConjunction()) {
                Collection childNodes = branchNode.getChildren();

                sb.append("(&");
                for (Iterator i=childNodes.iterator(); i.hasNext(); ) {
                    ExprNode childNode = (ExprNode)i.next();
                    FilterTool.convert(childNode, sb);
                }
                sb.append(")");

            } else if (branchNode.isDisjunction()) {
                Collection childNodes = branchNode.getChildren();

                sb.append("(|");
                for (Iterator i=childNodes.iterator(); i.hasNext(); ) {
                    ExprNode childNode = (ExprNode)i.next();
                    FilterTool.convert(childNode, sb);
                }
                sb.append(")");
            }

        } else if (node instanceof SubstringNode) {
            SubstringNode substringNode = (SubstringNode)node;

            sb.append("(");
            sb.append(substringNode.getAttribute());
            sb.append("=");

            if (substringNode.getInitial() != null) {
                sb.append(substringNode.getInitial());
            }

            sb.append("*");

            for (Iterator i=substringNode.getAny().iterator(); i.hasNext(); ) {
                String any = (String)i.next();

                sb.append(any);
                sb.append("*");
            }

            if (substringNode.getFinal() != null) {
                sb.append(substringNode.getFinal());
            }
            sb.append(")");
        }
    }
}
