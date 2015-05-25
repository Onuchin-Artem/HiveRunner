package com.klarna.jdbc;

import org.datanucleus.store.rdbms.datasource.dbcp.DelegatingResultSet;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by aonuchin on 16.01.15.
 */
public class HiveRunnerResultSet extends DelegatingResultSet {
    public HiveRunnerResultSet(Statement stmt) {
        super(stmt, Mockito.mock(ResultSet.class, new NotSupportedAnswer()));
    }
}
