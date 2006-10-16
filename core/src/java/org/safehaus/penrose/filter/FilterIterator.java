/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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
