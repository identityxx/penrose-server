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

import java.util.Stack;

/**
 * @author Endi S. Dewata
 */
public class FilterProcessor {

    public FilterProcessor() {
    }

    public void process(Filter filter) throws Exception {
        Stack<Filter> path = new Stack<Filter>();
        process(path, filter);
    }

    public void process(Stack<Filter> path, Filter filter) throws Exception {

        if (filter instanceof AndFilter) {
            AndFilter af = (AndFilter)filter;
            path.push(af);
            for (Filter f2 : af.getFilters()) {
                process(path, f2);
            }
            path.pop();

        } else if (filter instanceof OrFilter) {
            OrFilter af = (OrFilter)filter;
            path.push(af);
            for (Filter f2 : af.getFilters()) {
                process(path, f2);
            }
            path.pop();

        } else if (filter instanceof NotFilter) {
            NotFilter nf = (NotFilter)filter;
            path.push(nf);
            process(path, nf.getFilter());
            path.pop();
        }
    }
}
