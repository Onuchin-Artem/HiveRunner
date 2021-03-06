package com.klarna.jdbc;

import org.apache.hadoop.hive.service.HiveServer;
import org.datanucleus.store.rdbms.datasource.dbcp.DelegatingConnection;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;


public abstract class HiveRunnerConnection extends DelegatingConnection implements Connection {
    private boolean isClosed = false;
    private SQLWarning warning = null;
    private final HiveServer.HiveServerHandler client;

    public HiveRunnerConnection(HiveServer.HiveServerHandler client) {
        super(Mockito.mock(Connection.class, new NotSupportedAnswer()));
        this.client = client;
    }

    public void clearWarnings() throws SQLException {
        warning = null;
    }


    public void close() throws SQLException {
        isClosed = true;
    }

    private void checkClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("Connection is closed");
        }
    }

    public Statement createStatement() throws SQLException {
        checkClosed();
        return new HiveRunnerStatement(this, client);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        checkClosed();
        return new HiveRunnerStatement(this, client);
    }

    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    public String getCatalog() throws SQLException {
        return "";
    }


    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }


    public SQLWarning getWarnings() throws SQLException {
        return warning;
    }

    public boolean isClosed() throws SQLException {
        return isClosed;
    }


    public boolean isReadOnly() throws SQLException {
        return false;
    }


    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new HivePreparedStatement(this, client, sql);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        return new HivePreparedStatement(this, client, sql);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        return new HivePreparedStatement(this, client, sql);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (autoCommit) {
            throw new SQLException("enabling autocommit is not supported");
        }
    }

    public void setTransactionIsolation(int level) throws SQLException {
    }
}
