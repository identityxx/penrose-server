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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Stack;

/**
 * @author Endi S. Dewata
 */
public class FilterProcessor {

    public Logger log = LoggerFactory.getLogger(getClass());

    public FilterProcessor() {
    }

    public Filter process(Filter filter) throws Exception {
        Stack<Filter> path = new Stack<Filter>();
        return process(path, filter);
    }

    public Filter process(Stack<Filter> path, Filter filter) throws Exception {

        if (filter instanceof AndFilter) {
            AndFilter af = (AndFilter)filter;
            path.push(af);

            for (int i=0; i<af.getSize(); i++) {
                Filter oldFilter = af.getFilter(i);
                Filter newFilter = process(path, oldFilter);
                if (oldFilter != newFilter) af.setFilter(i, newFilter);
            }

            path.pop();

        } else if (filter instanceof OrFilter) {
            OrFilter of = (OrFilter)filter;
            path.push(of);

            for (int i=0; i<of.getSize(); i++) {
                Filter oldFilter = of.getFilter(i);
                Filter newFilter = process(path, oldFilter);
                if (oldFilter != newFilter) of.setFilter(i, newFilter);
            }

            path.pop();

        } else if (filter instanceof NotFilter) {
            NotFilter nf = (NotFilter)filter;
            path.push(nf);

            Filter oldFilter = nf.getFilter();
            Filter newFilter = process(path, oldFilter);
            if (oldFilter != newFilter) nf.setFilter(newFilter);

            path.pop();
        }

        return filter;
    }
}
