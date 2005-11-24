/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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
package org.safehaus.penrose;

import java.io.IOException;
import java.util.*;

public interface PenroseMBean {
	
	public int start() throws Exception;
	public void stop() throws Exception;

	public byte[] download(String filename) throws IOException;
	public void upload(String filename, byte content[]) throws IOException;
    public Collection listFiles(String directory) throws Exception;
    public Collection getLoggerNames(String path) throws Exception;

}
