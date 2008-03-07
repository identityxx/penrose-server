package org.safehaus.penrose.apacheds;

import org.apache.directory.shared.ldap.filter.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.filter.FilterTool;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ApacheDSFilterTool {

    public static Logger log = LoggerFactory.getLogger(ApacheDSFilterTool.class);

    public static String convert(ExprNode node) throws Exception {
        StringBuilder sb = new StringBuilder();
        convert(node, sb);
        return sb.toString();
    }

    public static void convert(ExprNode node, StringBuilder sb) throws Exception {
        log.debug("Converting filter: "+node);

        if (node == null) return;

        log.debug("Class: "+node.getClass().getName());

        if (node instanceof SimpleNode) {
            SimpleNode simpleNode = (SimpleNode)node;

            String attribute = simpleNode.getAttribute();
            String operation = AbstractExprNode.getOperationString(simpleNode.getAssertionType());
            Object value = simpleNode.getValue();

            sb.append("(");
            sb.append(attribute);
            sb.append(operation);
            sb.append(FilterTool.escape(value));
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
                convert(childNode, sb);
                sb.append(")");

            } else if (branchNode.isConjunction()) {
                Collection childNodes = branchNode.getChildren();

                sb.append("(&");
                for (Object o : childNodes) {
                    ExprNode childNode = (ExprNode) o;
                    convert(childNode, sb);
                }
                sb.append(")");

            } else if (branchNode.isDisjunction()) {
                Collection childNodes = branchNode.getChildren();

                sb.append("(|");
                for (Object o : childNodes) {
                    ExprNode childNode = (ExprNode) o;
                    convert(childNode, sb);
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

            for (Object o : substringNode.getAny()) {
                String any = (String) o;

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
