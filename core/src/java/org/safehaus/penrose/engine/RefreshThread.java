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
package org.safehaus.penrose.engine;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Administrator
 */
public class RefreshThread implements Runnable {

    public Logger log = LoggerFactory.getLogger(getClass());

    private Engine engine;

    public RefreshThread(Engine penrose) {
        this.engine = penrose;
    }

    public void run() {

        try {
            // sleep 5 minutes first, to allow server initialization
            Thread.sleep(5*60000);

        } catch (InterruptedException ex) {
            // ignore
        }

        while (!engine.isStopping()) {

            try {
                //handlerrceCache().refresh();
                Thread.sleep(2*60000); // sleep 2 minutes

            } catch (Exception ex) {
                // ignore
            }
        }

    }



}
