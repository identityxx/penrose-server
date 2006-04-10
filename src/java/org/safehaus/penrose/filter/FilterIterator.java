package org.safehaus.penrose.filter;

import java.util.Iterator;
import java.util.Stack;

/**
 * @author Endi S. Dewata
 */
public class FilterIterator {

    Filter filter;
    FilterVisitor visitor;

    public FilterIterator(Filter filter, FilterVisitor visitor) {
        this.filter = filter;
        this.visitor = visitor;
    }

    public void run() {
        Stack parents = new Stack();
        traverse(parents, filter);
    }

    public void traverse(Stack parents, Filter f) {

        visitor.preVisit(parents, f);

        if (f instanceof AndFilter) {
            AndFilter af = (AndFilter)f;
            parents.push(af);
            for (Iterator i=af.getFilters().iterator(); i.hasNext(); ) {
                Filter f2 = (Filter)i.next();
                traverse(parents, f2);
            }
            parents.pop();

        } else if (f instanceof OrFilter) {
            OrFilter af = (OrFilter)f;
            parents.push(af);
            for (Iterator i=af.getFilters().iterator(); i.hasNext(); ) {
                Filter f2 = (Filter)i.next();
                traverse(parents, f2);
            }
            parents.pop();

        } else if (f instanceof NotFilter) {
            NotFilter nf = (NotFilter)f;
            parents.push(nf);
            traverse(parents, nf.getFilter());
            parents.pop();
        }

        visitor.postVisit(parents, f);
    }
}
