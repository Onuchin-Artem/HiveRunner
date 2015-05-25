package com.klarna.jdbc;

import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.hive.service.cli.thrift.*;
import org.apache.thrift.TException;
import org.datanucleus.store.rdbms.datasource.dbcp.DelegatingConnection;
import org.datanucleus.store.rdbms.datasource.dbcp.DelegatingStatement;
import org.mockito.Mockito;

import java.sql.*;

/**
 * Created by aonuchin on 16.01.15.
 */
public class HiveRunnerStatement extends DelegatingStatement implements Statement {
    private int fetchSize;
    private ResultSet resultSet;
    private final HiveServer.HiveServerHandler client;
    private int maxRows;
    private SQLWarning warningChain;

    public HiveRunnerStatement(HiveRunnerConnection c, HiveServer.HiveServerHandler client) {
        super(c, Mockito.mock(Statement.class, new NotSupportedAnswer()));
        this.client = client;
    }


    public void cancel() throws SQLException {

    }

    public void clearWarnings() throws SQLException {
        warningChain = null;
    }

    public void close() throws SQLException {
        checkOpen();
        this._closed = true;
    }

    public boolean execute(String sql) throws SQLException {
        checkOpen();
        try {
            client.execute(sql);
            warningChain.setNextWarning(new SQLWarning(client.getStatusDetails()));
            resultSet = null;
            Schema schema = client.getSchema();
            if (schema.getFieldSchemasSize() == 0) {
                return false;
            }
            return true;
        } catch (TException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        checkOpen();
        if (!execute(sql)) {
            throw new SQLException("The query did not generate a result set!");
        }
        return resultSet;
    }

    public int executeUpdate(String sql) throws SQLException {
        execute(sql);
        return 0;
    }

    public int getFetchSize() throws SQLException {
        checkOpen();
        return fetchSize;
    }

    public int getMaxRows() throws SQLException {
        checkOpen();
        return maxRows;
    }

    public ResultSet getResultSet() throws SQLException {
        checkOpen();
        return resultSet;
    }

    public int getUpdateCount() throws SQLException {
        checkOpen();
        return 0;
    }


    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return warningChain;
    }

    public void setFetchSize(int rows) throws SQLException {
        checkOpen();
        fetchSize = rows;
    }

    public void setMaxRows(int max) throws SQLException {
        checkOpen();
        if (max < 0) {
            throw new SQLException("max must be >= 0");
        }
        maxRows = max;
    }
}
