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

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.jena.graph.impl.TransactionHandlerBase;
import org.apache.jena.shared.JenaException;

public class VirtTransactionHandler extends TransactionHandlerBase {

    private static final org.slf4j.Logger logger =
            org.slf4j.LoggerFactory.getLogger(VirtTransactionHandler.class);

    private VirtGraph graph = null;
    private Boolean m_transactionsSupported = null;

    public VirtTransactionHandler(VirtGraph _graph) {
        super();
        this.graph = _graph;
    }

    @Override
    public boolean transactionsSupported() {
        if (m_transactionsSupported != null) {
            return (m_transactionsSupported);
        }

        try {
            Connection c = graph.getConnection();
            if (c != null) {
                m_transactionsSupported = c.getMetaData()
                        .supportsMultipleTransactions();
                return (m_transactionsSupported);
            }
        } catch (Exception e) {
            throw new JenaException(e);
        }
        return (false);
    }

    @Override
    public void begin() {
        if (transactionsSupported()) {
            try {
                Connection c = graph.getConnection();
                if (c.getTransactionIsolation() != Connection.TRANSACTION_READ_COMMITTED) {
                    c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                }
                if (c.getAutoCommit()) {
                    c.setAutoCommit(false);
                }
            } catch (SQLException e) {
                throw new JenaException("Transaction begin failed: ", e);
            }
        } else {
            notSupported("begin transaction");
        }
    }

    @Override
    public void abort() {
        if (transactionsSupported()) {
            try {
                Connection c = graph.getConnection();
                c.rollback();
                c.commit();
                c.setAutoCommit(true);
            } catch (SQLException e) {
                throw new JenaException("Transaction rollback failed: ", e);
            }
        } else {
            notSupported("abort transaction");
        }
    }

    @Override
    public void commit() {
        if (transactionsSupported()) {
            try {
                Connection c = graph.getConnection();
                c.commit();
                c.setAutoCommit(true);
            } catch (SQLException e) {
                throw new JenaException("Transaction commit failed: ", e);
            }
        } else {
            notSupported("commit transaction");
        }
    }

    private void notSupported(String opName) {
        throw new UnsupportedOperationException(opName);
    }

}
