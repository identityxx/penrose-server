package org.safehaus.penrose.ldapbackend.mina;

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
