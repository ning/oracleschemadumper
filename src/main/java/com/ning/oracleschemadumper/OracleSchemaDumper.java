package com.ning.oracleschemadumper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.StringMapper;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class OracleSchemaDumper
{
    /**
     * Possible values include:
     *   CONSUMER GROUP
     *   EDITION
     *   EVALUATION CONTEXT
     *   FUNCTION
     *   INDEX
     *   INDEX PARTITION
     *   INDEXTYPE
     *   JAVA CLASS
     *   JAVA RESOURCE
     *   JOB CLASS
     *   LIBRARY
     *   LOB
     *   LOB PARTITION
     *   OPERATOR
     *   PACKAGE
     *   PACKAGE BODY
     *   PROCEDURE
     *   PROGRAM
     *   SCHEDULE
     *   SEQUENCE
     *   SYNONYM
     *   TABLE
     *   TABLE PARTITION
     *   TRIGGER
     *   TYPE
     *   VIEW
     *   WINDOW
     *   WINDOW GROUP
     *   XML SCHEMA
     */
    private static final List<String> NON_TABLE_TYPES = Arrays.asList("VIEW", "INDEX", "SEQUENCE", "PROCEDURE", "FUNCTION");
    private static final SimpleDateFormat ORACLE_DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private static final String TABLESPACE_REGEX = "TABLESPACE \"[^\"]*\"";

    @Parameter(names = { "-j", "--jdbc-url" }, description = "Database jdbc url", required = true)
    private String jdbcUrl;
    @Parameter(names = { "-u", "--username" }, description = "Database username", required = true)
    private String userName;
    @Parameter(names = { "-p", "--password" }, description = "Database password", required = true)
    private String password;
    @Parameter(names = { "-s", "--schema" }, description = "Database schemas to export; if not specified uses the schema for the database user")
    private List<String> schemas;
    @Parameter(names = { "-f", "--file" }, description = "The file to write the DDL SQL to; stdout if not specified")
    private String outputFile;
    @Parameter(names = { "-d", "--data-tables" }, description = "Names of the tables for which data shall be exported, too")
    private List<String> dataTables;
    @Parameter(names = { "--include-create-user" }, description = "Whether to include CREATE USER SQL for the user(s), off by default")
    private boolean includeCreateUser = false;
    @Parameter(names = { "--include-grants" }, description = "Whether to include GRANT SQL for the user(s), off by default")
    private boolean includeGrants = false;
    @Parameter(names = { "--include-dollar-tables" }, description = "Whether to include CREATE TABLE SQL for the tables with $ in their names, off by default")
    private boolean includeDollarTables = false;
    @Parameter(names = { "--include-tablespaces" }, description = "Whether to include DDL for tablespaces and create the tables in them, off by default")
    private boolean includeTablespaces = false;

    private String schemaSelector;

    public static void main(String[] args) throws IOException
    {
        final OracleSchemaDumper dumper = new OracleSchemaDumper();
        final JCommander commander = new JCommander(dumper);

        commander.setProgramName(OracleSchemaDumper.class.getSimpleName());
        try {
            commander.parse(args);
            dumper.init();
            dumper.run();
        }
        catch (ParameterException ex) {
            System.out.println(ex.getMessage());
            commander.usage();
        }
    }

    public void init()
    {
        if (schemas == null || schemas.size() == 0) {
            schemas = Arrays.asList(userName);
        }

        schemaSelector = joinForSql(schemas);
    }

    private static String joinForSql(List<String> identifiers)
    {
        StringBuilder builder = new StringBuilder();

        if (identifiers != null) {
            for (String str : identifiers) {
                if (builder.length() > 0) {
                    builder.append(",");
                }
                builder.append("'");
                builder.append(str.toUpperCase());
                builder.append("'");
            }
        }
        return builder.toString();
    }
    
    public void run() throws IOException
    {
        final PrintWriter output;

        if (outputFile == null) {
            output = new PrintWriter(System.out);
        }
        else {
            output = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));
        }
        
        IDBI dbi = new DBI(jdbcUrl, userName, password);

        dbi.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
                dumpUsers(handle, output);
                dumpTables(handle, output);
                dumpDdlForObjectTypes(handle, NON_TABLE_TYPES, output);
                if (dataTables != null) {
                    for (String dataTable : dataTables) {
                        dumpDataForTable(handle, dataTable, output);
                    }
                }
                return null;
            }
        });
        output.close();
    }

    private void dumpUsers(final Handle handle, final PrintWriter output)
    {
        String ddl;

        if (includeCreateUser) {
            ddl = handle.createQuery("SELECT trim(dbms_metadata.get_ddl('USER', username)) || '/' FROM all_users WHERE username IN (" + schemaSelector + ")")
                        .map(StringMapper.FIRST).first();
            if (ddl != null) {
                output.println(ddl + "\n");
            }
        }
        if (includeGrants) {
            ddl = handle.createQuery("SELECT trim(dbms_metadata.get_granted_ddl('ROLE_GRANT', username)) || '/' FROM all_users WHERE username IN (" + schemaSelector + ")")
                        .map(StringMapper.FIRST).first();
            if (ddl != null) {
                output.println(ddl + "\n");
            }
            ddl = handle.createQuery("SELECT trim(dbms_metadata.get_granted_ddl('SYSTEM_GRANT', username)) || '/' FROM all_users WHERE username IN (" + schemaSelector + ")")
                        .map(StringMapper.FIRST).first();
            if (ddl != null) {
                output.println(ddl + "\n");
            }
            ddl = handle.createQuery("SELECT trim(dbms_metadata.get_granted_ddl('OBJECT_GRANT', username)) || '/' FROM all_users WHERE username IN (" + schemaSelector + ")")
                        .map(StringMapper.FIRST).first();
            if (ddl != null) {
                output.println(ddl + "\n");
            }
        }
    }

    private void dumpTables(Handle handle, final PrintWriter output)
    {
        if (includeTablespaces) {
            // TODO:
            // * determine all used table spaces (from all_objects with owner ?)
            // * select dbms_metadata.get_ddl('TABLESPACE', tablespace_name) FROM dba_tablespaces WHERE tablespace_name IN (...);
        }

        DefaultDirectedGraph<String, DefaultEdge> dag = new DefaultDirectedGraph<String, DefaultEdge>(DefaultEdge.class);
        Set<String> tables = new HashSet<String>();
        Map<String, String> constraintsForTables = new HashMap<String, String>();

        ResultIterator<Map<String, Object>> tableIt = handle.createQuery("SELECT object_name name FROM all_objects " +
                                                                         "  WHERE owner IN (" + schemaSelector + ") " +
                                                                         "    AND object_type = 'TABLE' AND temporary = 'N' ORDER BY object_type DESC")
                                                            .setFetchSize(1000)
                                                            .iterator();
        while (tableIt.hasNext()) {
            Map<String, Object> table = tableIt.next();
            String tableName = table.get("name").toString();

            if (!tables.contains(tableName)) {
                if (includeDollarTables || tableName.indexOf('$') < 0) {
                    dag.addVertex(tableName);
                    tables.add(tableName);
                }
            }
        }
        tableIt.close();

        ResultIterator<Map<String, Object>> constraintIt = handle.createQuery("SELECT table_name, constraint_name FROM all_constraints " +
                                                                              "  WHERE owner IN (" + schemaSelector + ") " +
                                                                              "    AND constraint_type IN ('P', 'U', 'R')")
                                                                 .setFetchSize(1000)
                                                                 .iterator();

        while (constraintIt.hasNext()) {
            Map<String, Object> constraint = constraintIt.next();
            String tableName = constraint.get("table_name").toString();
            String constraintName = constraint.get("constraint_name").toString();

            if (!tables.contains(tableName)) {
                System.out.println("Found table " + tableName);
                dag.addVertex(tableName);
                tables.add(tableName);
            }
            constraintsForTables.put(constraintName, tableName);
        }
        constraintIt.close();
        constraintIt = handle.createQuery("SELECT table_name, constraint_name, r_constraint_name FROM all_constraints " +
                                          "  WHERE owner IN (" + schemaSelector + ") " +
                                          "    AND constraint_type IN ('R')")
                             .setFetchSize(1000)
                             .iterator();

        while (constraintIt.hasNext()) {
            Map<String, Object> constraint = constraintIt.next();
            String tableName = constraint.get("table_name").toString();
            String constraintName = constraint.get("constraint_name").toString();
            String remoteConstraintName = constraint.get("r_constraint_name").toString();
            String remoteTable = constraintsForTables.get(remoteConstraintName);

            if (remoteTable == null) {
                System.err.println("Constraint " + constraintName + " references a remote constraint not in the current schema: " + remoteConstraintName);
            }
            else if (!remoteTable.equalsIgnoreCase(tableName)) {
                dag.addEdge(remoteTable, tableName);
            }
        }
        constraintIt.close();

        CycleDetector<String, DefaultEdge> cycleDetector = new CycleDetector<String, DefaultEdge>(dag);

        if (cycleDetector.detectCycles()) {
            Set<String> cycles = cycleDetector.findCycles();
            while (!cycles.isEmpty()) {
                String cycle = cycles.iterator().next();
                Set<String> subCycle = cycleDetector.findCyclesContainingVertex(cycle);

                System.err.println("Found cyclic dependency: " + cycle);
                for (String sub : subCycle) {
                   System.err.println("   " + sub);
                   cycles.remove(sub);
                }
            }
        }
        else {
            TopologicalOrderIterator<String, DefaultEdge> orderIterator = new TopologicalOrderIterator<String, DefaultEdge>(dag);

            while (orderIterator.hasNext()) {
                dumpDdlForObject(handle, orderIterator.next(), "TABLE", output);
            }
        }
    }

    private void dumpDdlForObjectTypes(final Handle handle, final List<String> types, final PrintWriter output)
    {
        ResultIterator<Map<String, Object>> resultIt = handle.createQuery("SELECT object_type type, object_name name FROM all_objects " +
                                                                          "  WHERE owner IN (" + schemaSelector + ") " +
                                                                          "    AND object_type IN (" + joinForSql(types) + ") AND temporary = 'N' " +
                                                                          "  ORDER BY object_type DESC")
                                                             .setFetchSize(1000)
                                                             .iterator();
        
        while (resultIt.hasNext()) {
            Map<String, Object> row = resultIt.next();
            String type = row.get("type").toString();
            String name = row.get("name").toString();
            
            String ddl = handle.createQuery("SELECT trim(dbms_metadata.get_ddl('" + type + "','" + name + "')) || '/' FROM DUAL")
                               .map(StringMapper.FIRST).first();
            if (ddl != null) {
                if (!includeTablespaces) {
                    // remove tablespace part
                    ddl = ddl.replaceAll(TABLESPACE_REGEX, "");
                }
                output.println(ddl + "\n");
            }
        }
        resultIt.close();
    }
    
    private void dumpDdlForObject(final Handle handle, final String objectName, final String objectType, final PrintWriter output)
    {
        Map<String, Object> result = handle.createQuery("SELECT object_type type, object_name name FROM all_objects " +
                                                        "  WHERE owner IN (" + schemaSelector + ") " +
                                                        "    AND object_name = :object_name AND object_type = :object_type" +
                                                        "  ORDER BY object_type DESC")
                                           .bind("object_name", objectName.toUpperCase())
                                           .bind("object_type", objectType.toUpperCase())
                                           .first();

        if (result != null) {
            String type = result.get("type").toString();
            String name = result.get("name").toString();

            String ddl = handle.createQuery("SELECT trim(dbms_metadata.get_ddl('" + type + "','" + name + "')) || '/' FROM DUAL")
                               .map(StringMapper.FIRST).first();
            if (ddl != null) {
                if (!includeTablespaces) {
                    // remove tablespace part
                    ddl = ddl.replaceAll(TABLESPACE_REGEX, "");
                }
                output.println(ddl + "\n");
            }
        }
    }

    private void dumpDataForTable(final Handle handle, final String table, final PrintWriter output) throws SQLException
    {
        String realTableName = handle.createQuery("SELECT object_name FROM all_objects " +
                                                  "  WHERE owner IN (" + schemaSelector + ") " +
                                                  "    AND object_name = :object_name" +
                                                  "    AND object_type = 'TABLE'")
                                     .bind("object_name", table.toUpperCase())
                                     .map(StringMapper.FIRST)
                                     .first();

        if (realTableName == null) {
            System.err.println("The table " + table + " does not exist, is not a table or is not owned by one of the specified users");
            return;
        }
        
        final String sql = "SELECT * FROM " + table;

        ResultSet rs = handle.getConnection().prepareStatement(sql).executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        String[] columnNames = new String[metaData.getColumnCount()];
        int[] columnTypes = new int[metaData.getColumnCount()];

        for (int columnIdx = 0; columnIdx < columnNames.length; columnIdx++) {
            columnNames[columnIdx] = metaData.getColumnName(columnIdx + 1);
            columnTypes[columnIdx] = metaData.getColumnType(columnIdx + 1);
        }

        StringBuilder insertStmtFormatBuilder = new StringBuilder();

        insertStmtFormatBuilder.append("INSERT INTO \"");
        insertStmtFormatBuilder.append(realTableName);
        insertStmtFormatBuilder.append("\" (\"");

        for (int columnIdx = 0; columnIdx < columnNames.length; columnIdx++) {
            if (columnIdx > 0) {
                insertStmtFormatBuilder.append("\",\"");
            }
            insertStmtFormatBuilder.append(columnNames[columnIdx]);
        }
        insertStmtFormatBuilder.append("\") VALUES (%s) /");

        String insertStmtFormat = insertStmtFormatBuilder.toString();
        StringBuilder valuesBuilder = new StringBuilder();

        while (rs.next()) {
            valuesBuilder.setLength(0);
            for (int columnIdx = 0; columnIdx < columnNames.length; columnIdx++) {
                String nextValue = null;

                switch (columnTypes[columnIdx]) {
                    case Types.BIGINT:
                    case Types.BIT:
                    case Types.BOOLEAN:
                    case Types.DECIMAL:
                    case Types.DOUBLE:
                    case Types.FLOAT:
                    case Types.INTEGER:
                    case Types.NUMERIC:
                    case Types.REAL:
                    case Types.SMALLINT:
                    case Types.TINYINT:
                        nextValue = rs.getString(columnIdx + 1);
                        break;
                    case Types.DATE:
                        nextValue = toString(rs.getDate(columnIdx + 1));
                        break;
                    case Types.TIME:
                        nextValue = toString(rs.getTime(columnIdx + 1));
                        break;
                    case Types.TIMESTAMP:
                        nextValue = toString(rs.getTimestamp(columnIdx + 1));
                        break;
                    default:
                        nextValue = rs.getString(columnIdx + 1);
                        if (nextValue != null) {
                            nextValue = "'" + nextValue.replaceAll("'", "''") + "'";
                        }
                        break;
                }
                if (columnIdx > 0) {
                    valuesBuilder.append(",");
                }
                if (nextValue == null) {
                    valuesBuilder.append("null");
                }
                else {
                    valuesBuilder.append(nextValue);
                }
            }
            output.println(String.format(insertStmtFormat, valuesBuilder.toString()));
        }
        rs.close();
    }

    private static String toString(Date date)
    {
        return date == null ? null :  "TO_DATE('" + ORACLE_DATE_FORMAT.format(date) + "', 'YYYY/MM/DD HH24:MI:SS')";
    }
}
