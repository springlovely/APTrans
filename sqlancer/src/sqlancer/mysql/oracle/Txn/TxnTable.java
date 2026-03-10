package sqlancer.mysql.oracle.Txn;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLDataType;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.oracle.MySQLTxnGen;

public class TxnTable {
    private String tableName;
    private List<MySQLColumn> columns;
    private MySQLColumn primaryKey;
    private String createSql;
<<<<<<< HEAD
    private final boolean allowPrimaryKey;
=======
    private boolean allowPrimaryKey;
>>>>>>> e2d898d (添加APTrans核心代码)
    private boolean setPrimaryKey;
    private boolean setUniqueKey;
    private final StringBuilder sb = new StringBuilder();
    private final MySQLGlobalState globalState;
    private MySQLTable sqlTable;

    public TxnTable(){
        this.allowPrimaryKey = Randomly.getBoolean();
        this.globalState = null;
        this.columns = new ArrayList<>();
    }

    public TxnTable(MySQLGlobalState globalState) {
        this.allowPrimaryKey = Randomly.getBoolean();
        this.globalState = globalState;
        this.columns = new ArrayList<>();
        genRandomTable();
    }

<<<<<<< HEAD
=======
    public void setTableName(String tableName){
        this.tableName = tableName;
    }

    public void setCreateSql(String createSql){
        this.createSql = createSql;
        this.columns = new ArrayList<>();
        this.create_sql_parse();
    }

    public void create_sql_parse() {
        // 1. 提取括号内的列定义部分
        java.util.regex.Pattern columnDefPattern = java.util.regex.Pattern.compile("CREATE TABLE \\w+ \\((.+)\\);", java.util.regex.Pattern.DOTALL);
        java.util.regex.Matcher matcher = columnDefPattern.matcher(createSql);
        if (!matcher.find()) {
            throw new IllegalArgumentException("无法解析CREATE TABLE语句: " + createSql);
        }
    
        String columnDefs = matcher.group(1);
        // 使用零宽断言分割逗号，防止误伤 VARCHAR(10,2) 中的逗号
        String[] columnParts = columnDefs.split(",(?![^()]*\\))");
    
        for (String part : columnParts) {
            part = part.trim();
            // 跳过约束行
            if (part.startsWith("FOREIGN KEY") || part.startsWith("PRIMARY KEY (") || part.isEmpty()) {
                continue;
            }

            java.util.regex.Pattern columnPattern = java.util.regex.Pattern.compile("^(\\w+)\\s+([A-Za-z0-9()]+)(.*)$");
            java.util.regex.Matcher colMatcher = columnPattern.matcher(part);
    
            if (colMatcher.find()) {
                String colName = colMatcher.group(1);
                String rawType = colMatcher.group(2).trim(); // 此时 rawType 应该是 "BIGINT"
                String colOptions = colMatcher.group(3).trim(); // 此时 colOptions 应该是 "PRIMARY KEY NOT NULL"
    
                // 特殊处理: 如果类型后面紧跟 UNSIGNED (MySQL特有)，将其合并到类型中
                String colType = rawType;
                if (colOptions.toUpperCase().startsWith("UNSIGNED")) {
                    colType += " UNSIGNED";
                    colOptions = colOptions.substring("UNSIGNED".length()).trim();
                }
    
                // 构建列的属性
                Map<String, Object> columnOptions = new HashMap<>();
                List<MySQLColumn.ColumnOptions> optionsList = new ArrayList<>();
    
                // 解析约束 (检查 colOptions 字符串)
                String upperOptions = colOptions.toUpperCase();
                
                if (upperOptions.contains("NOT NULL")) {
                    optionsList.add(MySQLColumn.ColumnOptions.NOT_NULL);
                } else if (upperOptions.contains("NULL")) {
                    optionsList.add(MySQLColumn.ColumnOptions.NULL);
                }
    
                if (upperOptions.contains("UNIQUE")) {
                    optionsList.add(upperOptions.contains("KEY") ? 
                        MySQLColumn.ColumnOptions.UNIQUE_KEY : 
                        MySQLColumn.ColumnOptions.UNIQUE);
                }
    
                boolean isPrimaryKey = upperOptions.contains("PRIMARY KEY");
                if (isPrimaryKey) {
                    optionsList.add(MySQLColumn.ColumnOptions.PRIMARY_KEY);
                }
    
                // 确定数据类型 (现在传进去的 colType 不会包含 PRIMARY KEY 等杂质)
                MySQLDataType dataType = mapToMySQLDataType(colType);
    
                // 构建 Options 映射
                columnOptions.put("isPrimaryKey", isPrimaryKey);
                columnOptions.put("ColumnOptions", optionsList);
                columnOptions.put("trueType", colType);
                columnOptions.put("isUnsigned", colType.toUpperCase().contains("UNSIGNED"));
                columnOptions.put("isForeignKey", false);
    
                // 创建并添加列
                MySQLColumn column = new MySQLColumn(colName, dataType, columnOptions, 4);
                columns.add(column);
    
                if (isPrimaryKey) {
                    this.primaryKey = column;
                }
            }
        }
    
        // 设置表的列
        this.setColumns(columns);
    
        // 创建MySQLTable对象并关联
        sqlTable = new MySQLTable(tableName, columns, null, null);
        for (MySQLColumn column : columns) {
            column.setTable(sqlTable);
        }
    }

    public MySQLDataType mapToMySQLDataType(String typeStr) {
        typeStr = typeStr.toUpperCase().replace(" UNSIGNED", "").split("\\(")[0];
        
        switch (typeStr) {
            case "INT":
            case "BIGINT":
                return MySQLDataType.INT;
            case "DECIMAL":
                return MySQLDataType.DECIMAL;
            case "VARCHAR":
            case "TEXT":
                return MySQLDataType.VARCHAR;
            case "FLOAT":
                return MySQLDataType.FLOAT;
            case "DOUBLE":
                return MySQLDataType.DOUBLE;
            case "BOOLEAN":
                return MySQLDataType.BOOLEAN;
            default:
                throw new IllegalArgumentException("不支持的数据类型: " + typeStr);
        }
    }

>>>>>>> e2d898d (添加APTrans核心代码)
    public boolean isSetUniqueKey() {
        return setPrimaryKey || setUniqueKey;
    }

    public boolean isSetPrimaryKey() {
        return setPrimaryKey;
    }

    public String getTableName() {
        return tableName;
    }

    public List<MySQLColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<MySQLColumn> columns) {
        this.columns = columns;
    }

    public String getCreateSql() {
        return createSql;
    }

<<<<<<< HEAD

=======
>>>>>>> e2d898d (添加APTrans核心代码)
    public MySQLColumn getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Table Name: ").append(tableName).append("\n");
        sb.append("Columns: ").append(columns).append("\n");
        sb.append("Create SQL: ").append(createSql).append("\n");
        return sb.toString();
    }

    public MySQLTable getMySQLTable() {
        return sqlTable;
    }

    private void genRandomTable() {
        tableName = "t" + RandomStringUtils.randomAlphanumeric(7);
        sb.append("CREATE TABLE ").append(tableName).append(" (");
        appendIDColumn();
        for (int i = 0; i < 3; i++) {
            if (i > 0)
                sb.append(", ");
            appendColumn(i);
        }
        sqlTable = new MySQLTable(tableName, columns, null, null);

        for (MySQLColumn column : columns) {
            column.setTable(sqlTable);
            if (column.isForeignKey()) {
                MySQLColumn.ForeignKeyReference foreignKeyReference = column.getForeignKeyReference();
                sb.append(", FOREIGN KEY(");
                sb.append(column.getName());
                sb.append(") REFERENCES ");
                sb.append(foreignKeyReference.getReferencedTableName());
                sb.append("(");
                sb.append(foreignKeyReference.getReferencedColumnName());
                sb.append(")");
                MySQLTxnGen.foreign_key_count++;
            }
        }
        sb.append(");");
        createSql = sb.toString();
    }

    private void appendIDColumn() {
        sb.append("ID INT, VAL INT, ");
    }

    private void appendColumn(int columnId) {
        String columnName = DBMSCommon.createColumnName(columnId);
        sb.append(columnName).append(" ");
        Map<String, Object> columnOption_Dict = new HashMap<>();
        MySQLDataType randomType = MySQLDataType.getRandom();
        boolean isUnsigned = false;
        if (Randomly.getBoolean() && randomType == MySQLDataType.INT)
            isUnsigned = true;
        String True_Type = appendType(randomType, isUnsigned);
        sb.append(" ");
        columnOption_Dict.put("isUnsigned", isUnsigned);
        columnOption_Dict.put("trueType", True_Type);
        columnOption_Dict.putAll(appendColumnOption(randomType));
        boolean isUnique = columnOption_Dict.get("ColumnOptions").toString().contains("UNIQUE_KEY") || columnOption_Dict.get("ColumnOptions").toString().contains("UNIQUE") || columnOption_Dict.get("ColumnOptions").toString().contains("PRIMARY_KEY");
        columnOption_Dict.putAll(appendForeignKey(True_Type, isUnique));
        MySQLColumn column = new MySQLColumn(columnName, randomType, columnOption_Dict, 4);
        if (column.isPrimaryKey()) {
            primaryKey = column;
        }
        columns.add(column);
    }

    private Map<String, Object> appendForeignKey(String randomType, boolean isUnique) {
        if (isUnique || Randomly.getBoolean() || randomType.contains("BOOLEAN")) {
            return Map.of("isForeignKey", false, "referencedTable", new Object(), "referencedColumn", new Object());
        }
        List<MySQLTable> tables = globalState.getSchema().getDatabaseTables();
        if (!tables.isEmpty() && Randomly.getBoolean()) {
            MySQLTable candidate_table = Randomly.fromList(tables);
            List<MySQLColumn> candidate_columns = candidate_table.getColumns();
            candidate_columns = candidate_columns.stream().filter(c -> c.getTrueType().equals(randomType)).collect(Collectors.toList());
            candidate_columns = candidate_columns.stream().filter(MySQLColumn::isUnique).collect(Collectors.toList());
            if (!candidate_columns.isEmpty()) {
                MySQLColumn select_column = Randomly.fromList(candidate_columns);
                return Map.of("isForeignKey", true, "referencedTable", candidate_table.getName(), "referencedColumn", select_column.getName());
            }
        }
        return Map.of("isForeignKey", false, "referencedTable", new Object(), "referencedColumn", new Object());
    }

    private String appendType(MySQLDataType randomType, boolean isUnsigned) {
        String True_Type = "";
        switch (randomType) {
            case DECIMAL:
                True_Type = "DECIMAL";
                StringBuilder sb1 = new StringBuilder();
                optionallyAddPrecisionAndScale(sb1, true);
                True_Type = True_Type + sb1.toString();
                break;
            case INT:
                True_Type = Randomly.fromOptions("INT", "BIGINT");
                break;
            case VARCHAR:
                True_Type = Randomly.fromOptions("VARCHAR(100)", "TEXT");
                break;
            case FLOAT:
                // True_Type = "FLOAT";
                // break;
            case DOUBLE:
                True_Type = "DOUBLE";
                break;
            case BOOLEAN:
                True_Type = "BOOLEAN";
                break;
            default:
                throw new AssertionError();
        }
        if (randomType.isNumeric() && isUnsigned) {
            True_Type = True_Type + " UNSIGNED";
        }
        sb.append(True_Type);
        return True_Type;
    }

    private Map<String, Object> appendColumnOption(MySQLDataType type) {
        if (type == MySQLDataType.BOOLEAN){
            return Map.of("isPrimaryKey", false, "ColumnOptions", new ArrayList<>());
        }
        Map<String, Object> Settings_Map = new HashMap<>();
        boolean isTextType = type == MySQLDataType.VARCHAR;
        boolean isNull = false;
        boolean columnHasPrimaryKey = false;
        List<MySQLColumn.ColumnOptions> columnOptions = Randomly.subset(MySQLColumn.ColumnOptions.values());
        List<MySQLColumn.ColumnOptions> true_columnOptions = new ArrayList<>();
        columnOptions.remove(MySQLColumn.ColumnOptions.NOT_NULL);
        columnOptions.remove(MySQLColumn.ColumnOptions.NULL);
        columnOptions.remove(MySQLColumn.ColumnOptions.UNIQUE_KEY);
        if (isTextType) {
            columnOptions.remove(MySQLColumn.ColumnOptions.PRIMARY_KEY);
            columnOptions.remove(MySQLColumn.ColumnOptions.UNIQUE);
        }
        for (MySQLColumn.ColumnOptions o : columnOptions) {
            sb.append(" ");
            switch (o) {
            case NULL_OR_NOT_NULL:
                if (!columnHasPrimaryKey) {
                    if (Randomly.getBoolean() && !setPrimaryKey) {
                        sb.append("NULL");
                        true_columnOptions.add(MySQLColumn.ColumnOptions.NULL);
                    }
                    isNull = true;
                } else {
                    sb.append("NOT NULL");
                    true_columnOptions.add(MySQLColumn.ColumnOptions.NOT_NULL);
                }
                break;
            case UNIQUE:
                if (Randomly.getBoolean() && !setPrimaryKey) {
                    sb.append("UNIQUE KEY");
                    true_columnOptions.add(MySQLColumn.ColumnOptions.UNIQUE_KEY);
                } else {
                    if (!setPrimaryKey){
                        sb.append("UNIQUE");
                        true_columnOptions.add(MySQLColumn.ColumnOptions.UNIQUE);
                    }
                }
                setUniqueKey = true;
                break;
            case PRIMARY_KEY:
                if (allowPrimaryKey && !setPrimaryKey && !isNull) {
                    sb.append("PRIMARY KEY");
                    setPrimaryKey = true;
                    columnHasPrimaryKey = true;
                    true_columnOptions.add(MySQLColumn.ColumnOptions.PRIMARY_KEY);
                }
                break;
            default:
                throw new AssertionError();
            }
        }
        Settings_Map.put("isPrimaryKey", columnHasPrimaryKey);
        Settings_Map.put("ColumnOptions", true_columnOptions);
        return Settings_Map;
    }

    private void optionallyAddPrecisionAndScale(StringBuilder sb, boolean isDECIMAL) {
        if (isDECIMAL || Randomly.getBoolean()) {
            sb.append("(");
            long nCandidate = Randomly.getNotCachedInteger(2, 30);
            long n = Math.min(nCandidate, 32);
            long m = Randomly.getNotCachedInteger((int) n + 10, 65);
            sb.append(m);
            sb.append(", ");
            sb.append(n);
            sb.append(")");
        }
    }
}
