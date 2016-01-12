/*
 *  $Id$
 *
 *  This file is part of the OpenLink Software Virtuoso Open-Source (VOS)
 *  project.
 *
 *  Copyright (C) 1998-2012 OpenLink Software
 *
 *  This project is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the
 *  Free Software Foundation; only version 2 of the License, dated June 1991.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 *
 */

package virtuoso.jena.driver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.update.UpdateException;

public class VirtuosoUpdateRequest {

    private static final org.slf4j.Logger logger =
            org.slf4j.LoggerFactory.getLogger(VirtuosoUpdateRequest.class);

    private List<Object> requests = new ArrayList<>();
    private VirtGraph graph;
    private String virt_query;

    java.sql.Statement stmt = null;

    public VirtuosoUpdateRequest(VirtGraph _graph) {
        graph = _graph;
    }

    public VirtuosoUpdateRequest(String query, VirtGraph _graph) {
        this(_graph);
        virt_query = query;
        requests.add(query);
    }

    public void exec() {
        try {
            stmt = graph.createStatement();

            for (Object request : requests) {
                String query = "sparql\n " + String.valueOf(request);
                stmt.execute(query);
            }

            stmt.close();
            stmt = null;
        } catch (Exception e) {
            throw new UpdateException("Convert results are FAILED.:", e);
        }

    }

    public void addUpdate(String update) {
        requests.add(update);
    }

    public Iterator iterator() {
        return requests.iterator();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();

        for (Object request : requests) {
            b.append((String) request);
            b.append("\n");
        }
        return b.toString();
    }

    public void output(IndentedWriter out) {
        boolean first = true;
        out.println();

        for (Object request : requests) {
            if (!first)
                out.println("    # ----------------");
            else
                first = false;
            System.out.println((String) request);
            out.ensureStartOfLine();
        }

    }

}
