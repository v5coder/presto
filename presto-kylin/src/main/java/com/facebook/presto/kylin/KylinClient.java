package com.facebook.presto.kylin;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Pattern;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class KylinClient {

    private static final Logger log = Logger.get(KylinClient.class);

    private static final Map<Type, String> SQL_TYPES = ImmutableMap.<Type, String>builder()
            .put(BOOLEAN, "boolean")
            .put(BIGINT, "bigint")
            .put(INTEGER, "integer")
            .put(SMALLINT, "smallint")
            .put(TINYINT, "tinyint")
            .put(DOUBLE, "double precision")
            .put(REAL, "real")
            .put(VARBINARY, "varbinary")
            .put(DATE, "date")
            .put(TIME, "time")
            .put(TIME_WITH_TIME_ZONE, "time with timezone")
            .put(TIMESTAMP, "timestamp")
            .put(TIMESTAMP_WITH_TIME_ZONE, "timestamp with timezone")
            .build();

    protected final String connectorId;
    protected final ConnectionFactory connectionFactory;
    protected final String identifierQuote;

    @Inject
    public KylinClient(KylinConnectorId connectorId, KylinConfig config) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        requireNonNull(config, "config is null"); // currently unused, retained as parameter for future extensions
        this.identifierQuote = "`";
        this.connectionFactory = connectionFactory(config);
    }

    private static ConnectionFactory connectionFactory(KylinConfig config) {
        return new DriverConnectionFactory(config);
    }

    public Set<String> getSchemaNames() {
        try {
            KylinClientHelper helper = connectionFactory.openConnection();
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            String projects = helper.listProjects();
            JSONArray array = JSON.parseArray(projects);
            for (int i = 0; i < array.size(); i++) {
                JSONObject object = array.getJSONObject(i);
                schemaNames.add(String.valueOf(object.get("name")));
            }
            return schemaNames.build();
        } catch (Exception e) {
            throw new PrestoException(KylinErrorCode.KYLIN_ERROR, e);
        }
    }

    public List<SchemaTableName> getTableNames(@Nullable String schema) {
        try {
            KylinClientHelper helper = connectionFactory.openConnection();
            ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
            String projects = helper.listCubes(schema);
            JSONArray array = JSON.parseArray(projects);
            for (int i = 0; i < array.size(); i++) {
                JSONObject object = array.getJSONObject(i);
                if (String.valueOf(object.get("status")).equals("READY")) {
                    list.add(getSchemaTableName(String.valueOf(object.get("project")), String.valueOf(object.get("name"))));
                }
            }
            return list.build();
        } catch (Exception e) {
            throw new PrestoException(KylinErrorCode.KYLIN_ERROR, e);
        }
    }

    protected SchemaTableName getSchemaTableName(String schemaName, String tableName) {
        return new SchemaTableName(schemaName, tableName);
    }

    @Nullable
    public KylinTableHandle getTableHandle(SchemaTableName schemaTableName) {
        try {
            KylinClientHelper helper = connectionFactory.openConnection();
            String cubeDesc = helper.getCube(schemaTableName.getTableName());
            JSONArray cubeInfos = JSONArray.parseArray(cubeDesc);
            List<KylinTableHandle> tableHandles = new ArrayList<>();
            for (int i = 0; i < cubeInfos.size(); i++) {
                JSONObject dimension = cubeInfos.getJSONObject(i);
                tableHandles.add(new KylinTableHandle(
                        connectorId,
                        schemaTableName,
                        String.valueOf(dimension.get("name")),
                        schemaTableName.getSchemaName(),
                        schemaTableName.getTableName()));
            }
            if (tableHandles.isEmpty()) {
                return null;
            }
            if (tableHandles.size() > 1) {
                throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
            }
            return getOnlyElement(tableHandles);
        } catch (Exception e) {
            throw new PrestoException(KylinErrorCode.KYLIN_ERROR, e);
        }
    }

    public List<KylinColumnHandle> getColumns(ConnectorSession session, KylinTableHandle tableHandle) {
        try {
            KylinClientHelper helper = connectionFactory.openConnection();
            String cubeDesc = helper.getCube(tableHandle.getTableName());
            JSONArray cubeInfos = JSONArray.parseArray(cubeDesc);
            JSONObject cubeInfo = cubeInfos.getJSONObject(0);
            List<KylinColumnHandle> columns = new ArrayList<>();
            // 遍历维度信息
            JSONArray dimensions = cubeInfo.getJSONArray("dimensions");
            for (int i = 0; i < dimensions.size(); i++) {
                JSONObject dimension = dimensions.getJSONObject(i);
                KylinTypeHandle typeHandle = new KylinTypeHandle(12, 0, 0);
                Optional<ReadMapping> columnMapping = toPrestoType(session, typeHandle);
                // skip unsupported column types
                if (columnMapping.isPresent()) {
                    String columnName = dimension.getString("column");
                    columns.add(new KylinColumnHandle(connectorId, columnName, typeHandle, columnMapping.get().getType()));
                }
            }
            JSONArray measures = cubeInfo.getJSONArray("measures");
            for (int i = 0; i < measures.size(); i++) {
                JSONObject measure = measures.getJSONObject(i);
                KylinTypeHandle typeHandle = new KylinTypeHandle(12, 0, 0);
                Optional<ReadMapping> columnMapping = toPrestoType(session, typeHandle);
                // skip unsupported column types
                if (columnMapping.isPresent()) {
                    JSONObject function = measure.getJSONObject("function");
                    JSONObject parameter = function.getJSONObject("parameter");
                    String columnName = parameter.getString("value");
                    Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
                    if (!pattern.matcher(columnName).matches()) {
                        String[] column = columnName.split("\\.");
                        KylinColumnHandle columnHandle =
                                new KylinColumnHandle(connectorId, column.length == 0 ? column[0] : column[1], typeHandle, columnMapping.get().getType());
                        if (!columns.contains(columnHandle)) {
                            columns.add(columnHandle);
                        }
                    }
                }
            }
            if (columns.isEmpty()) {
                throw new TableNotFoundException(tableHandle.getSchemaTableName());
            }
            return ImmutableList.copyOf(columns);
        } catch (Exception e) {
            throw new PrestoException(KylinErrorCode.KYLIN_ERROR, e);
        }
    }

    public Optional<ReadMapping> toPrestoType(ConnectorSession session, KylinTypeHandle typeHandle) {
        return StandardReadMappings.kylinTypeToPrestoType(typeHandle);
    }

    public PreparedStatement buildSql(KylinClientHelper connection,
                                      KylinSplit split,
                                      List<KylinColumnHandle> columnHandles) throws SQLException {
        return null;/*new QueryBuilder(identifierQuote).buildSql(
                this,
                connection,
                split.getCatalogName(),
                split.getSchemaName(),
                split.getTableName(),
                columnHandles,
                split.getTupleDomain());*/
    }

    public PreparedStatement getPreparedStatement(Connection connection, String sql) throws SQLException {
        return connection.prepareStatement(sql);
    }

    public ConnectorSplitSource getSplits(KylinTableLayoutHandle layoutHandle) {

        KylinTableHandle tableHandle = layoutHandle.getTable();
        KylinSplit jdbcSplit = new KylinSplit(
                connectorId,
                tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                layoutHandle.getTupleDomain());
        return new FixedSplitSource(ImmutableList.of(jdbcSplit));
    }

    protected String quoted(String catalog, String schema, String table) {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(catalog)) {
            sb.append(quoted(catalog)).append(".");
        }
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema)).append(".");
        }
        sb.append(quoted(table));
        return sb.toString();
    }

    protected String quoted(String name) {
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }

    public boolean schemaExists(String schema) {
        return getSchemaNames().contains(schema);
    }

    protected void execute(Connection connection, String query)
            throws SQLException {
        try (Statement statement = connection.createStatement()) {
            log.debug("Execute: %s", query);
            statement.execute(query);
        }
    }

    public KylinOutputTableHandle beginCreateTable(ConnectorTableMetadata tableMetadata) {
        return beginWriteTable(tableMetadata);
    }

    private KylinOutputTableHandle beginWriteTable(ConnectorTableMetadata tableMetadata) {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schema = schemaTableName.getSchemaName();
        String table = schemaTableName.getTableName();

        if (!getSchemaNames().contains(schema)) {
            throw new PrestoException(NOT_FOUND, "Schema not found: " + schema);
        }

        /*try (Connection connection = connectionFactory.openConnection()) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            if (uppercase) {
                schema = schema.toUpperCase(ENGLISH);
                table = table.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();

            String temporaryName = generateTemporaryTableName();
            StringBuilder sql = new StringBuilder()
                    .append("CREATE TABLE ")
                    .append(quoted(catalog, schema, temporaryName))
                    .append(" (");
            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                columnList.add(new StringBuilder()
                        .append(quoted(columnName))
                        .append(" ")
                        .append(toSqlType(column.getType()))
                        .toString());
            }
            Joiner.on(", ").appendTo(sql, columnList.build());
            sql.append(")");

            execute(connection, sql.toString());

            return new KylinOutputTableHandle(
                    connectorId,
                    catalog,
                    schema,
                    table,
                    columnNames.build(),
                    columnTypes.build(),
                    temporaryName);
        } catch (SQLException e) {
            throw new PrestoException(KylinErrorCode.KYLIN_ERROR, e);
        }*/
        return null;
    }

    protected String generateTemporaryTableName() {
        return "tmp_presto_" + UUID.randomUUID().toString().replace("-", "");
    }

    protected String toSqlType(Type type) {
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded()) {
                return "varchar";
            }
            return "varchar(" + varcharType.getLengthSafe() + ")";
        }
        if (type instanceof CharType) {
            if (((CharType) type).getLength() == CharType.MAX_LENGTH) {
                return "char";
            }
            return "char(" + ((CharType) type).getLength() + ")";
        }
        if (type instanceof DecimalType) {
            return format("decimal(%s, %s)", ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
        }

        String sqlType = SQL_TYPES.get(type);
        if (sqlType != null) {
            return sqlType;
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    public KylinOutputTableHandle beginInsertTable(ConnectorTableMetadata tableMetadata) {
        return beginWriteTable(tableMetadata);
    }

    public KylinClientHelper getConnection(KylinSplit split) {
        return connectionFactory.openConnection();
    }

    public KylinClientHelper getConnection(KylinOutputTableHandle handle){
        return connectionFactory.openConnection();
    }
}