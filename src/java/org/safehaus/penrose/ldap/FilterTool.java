package org.safehaus.penrose.ldap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.filter.*;
import org.apache.directory.shared.ldap.filter.*;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class FilterTool {

    public static Logger log = LoggerFactory.getLogger(FilterTool.class);

    public static Filter convert(ExprNode node) throws Exception {
        log.debug("Converting filter: "+node);

        if (node == null) return null;

        log.debug("Class: "+node.getClass().getName());

        if (node instanceof SimpleNode) {
            SimpleNode simpleNode = (SimpleNode)node;

            String attribute = simpleNode.getAttribute();
            String operation = AbstractExprNode.getOperationString(simpleNode.getAssertionType());
            String value = simpleNode.getValue();
            return new SimpleFilter(attribute, operation, value);

        } else if (node instanceof PresenceNode) {
            PresenceNode presenceNode = (PresenceNode)node;

            String attribute = presenceNode.getAttribute();
            return new PresentFilter(attribute);

        } else if (node instanceof BranchNode) {
            BranchNode branchNode = (BranchNode)node;

            if (branchNode.isNegation()) {
                ExprNode childNode = branchNode.getChild();
                Filter childFilter = convert(childNode);
                return new NotFilter(childFilter);

            } else if (branchNode.isConjunction()) {
                AndFilter filter = new AndFilter();

                Collection childNodes = branchNode.getChildren();
                for (Iterator i=childNodes.iterator(); i.hasNext(); ) {
                    ExprNode childNode = (ExprNode)i.next();
                    Filter childFilter = convert(childNode);
                    filter.addFilter(childFilter);
                }

                return filter;

            } else if (branchNode.isDisjunction()) {
                OrFilter filter = new OrFilter();

                Collection childNodes = branchNode.getChildren();
                for (Iterator i=childNodes.iterator(); i.hasNext(); ) {
                    ExprNode childNode = (ExprNode)i.next();
                    Filter childFilter = convert(childNode);
                    filter.addFilter(childFilter);
                }

                return filter;
            }

        } else if (node instanceof SubstringNode) {
            SubstringNode substringNode = (SubstringNode)node;

            SubstringFilter filter = new SubstringFilter();
            filter.setAttribute(substringNode.getAttribute());

            if (substringNode.getInitial() != null) filter.addSubstring(substringNode.getInitial());
            filter.addSubstring(SubstringFilter.STAR);

            for (Iterator i=substringNode.getAny().iterator(); i.hasNext(); ) {
                String any = (String)i.next();
                filter.addSubstring(any);
                filter.addSubstring(SubstringFilter.STAR);
            }

            if (substringNode.getFinal() != null) filter.addSubstring(substringNode.getFinal());
            substringNode.getFinal();

            return filter;
        }

        return null;
    }
}
