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

import org.apache.jena.graph.*;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @deprecated from jena 3.X.X use instead {@link VirtGraph}
 */
public class VirtBulkUpdateHandler extends GraphBase {

    private static final org.slf4j.Logger logger =
            org.slf4j.LoggerFactory.getLogger(VirtBulkUpdateHandler.class);

    private final Graph graph;

    public VirtBulkUpdateHandler(VirtGraph parent) {
        super();
        graph = parent;
    }

    // --java5 or newer @Override
    public void add(Triple[] triples) {
        addIterator(Arrays.asList(triples).iterator(), false);
        gem.notifyAddArray(graph, triples);
    }

    // --java5 or newer @Override
    protected void add(List<Triple> triples, boolean notify) {
        addIterator(triples.iterator(), false);
        if (notify)
            gem.notifyAddList(graph, triples);
    }

    // --java5 or newer @Override
    public void addIterator(Iterator<Triple> it, boolean notify) {
        VirtGraph _graph = (VirtGraph) this.graph;
        List<Triple> list;
        if (notify) list = new ArrayList<>();
        else list = null;

        _graph = _graph.prepareGraphConnection(_graph, it, list);

        if (notify)
            gem.notifyAddIterator(_graph, list);
    }

    public void delete(Triple[] triples) {
        deleteIterator(Arrays.asList(triples).iterator(), false);
        gem.notifyDeleteArray(graph, triples);
    }

    protected void delete(List<Triple> triples, boolean notify) {
        deleteIterator(triples.iterator(), false);
        if (notify)
            gem.notifyDeleteList(graph, triples);
    }

    public void deleteIterator(Iterator<Triple> it, boolean notify) {
        VirtGraph _graph = (VirtGraph) this.graph;
        List<Triple> list;
        if (notify) list = new ArrayList<>();
        else list = null;

        _graph = _graph.prepareGraphConnection(_graph, it, list);

        if (notify)
            gem.notifyDeleteIterator(_graph, list);
    }

    public void removeAll() {
        VirtGraph _graph = (VirtGraph) this.graph;
        _graph.clearGraph(_graph.getGraphName());
    }

    @Override
    public void remove(Node s, Node p, Node o) {
        VirtGraph _graph = (VirtGraph) this.graph;
        _graph.delete_match(Triple.createMatch(s, p, o));
        gem.notifyEvent(_graph, GraphEvents.remove(s, p, o));
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {
        return super.graphBaseFind(
                triplePattern.getMatchSubject(), triplePattern.getMatchPredicate(), triplePattern.getMatchObject());
    }


}
