package sqlancer.mysql.oracle;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
<<<<<<< HEAD
import java.util.ArrayList;
import java.util.List;
=======
import java.util.regex.*;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
>>>>>>> e2d898d (添加APTrans核心代码)

import sqlancer.Randomly;
import sqlancer.Txn_constant;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.oracle.Txn.TxnTable;
import sqlancer.mysql.oracle.Txn.Cases;

public class MySQLTxnGen {
    private final MySQLGlobalState state;
    public List<TxnTable> tables = new ArrayList<>();
    public static int foreign_key_count = 0;
    public int table_num = 3;
    public int txn_num = 1000;
    public String save_path = "./cases";

    public MySQLTxnGen(MySQLGlobalState globalState, int table_num, int txn_num, String save_path) {
        this.table_num = table_num;
        this.save_path = save_path;
        this.txn_num = txn_num;
        this.state = globalState;
    }

<<<<<<< HEAD
=======
    public void MySQLDatabaseGenerate(String DatabasePath) throws Exception {
        Path dirPath = Paths.get(DatabasePath);
        
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        String fileName = "Database_" + System.currentTimeMillis() + Randomly.getNotCachedInteger(0, 10000) + ".sql";
        Path filePath = dirPath.resolve(fileName);

        state.getSchema().clearTable();
        tables.clear();

        for (int i = 0; i < table_num; i++) {
            TxnTable table = new TxnTable(state);
            MySQLTable sqlTable = table.getMySQLTable();
            tables.add(table);
            state.getSchema().addTable(sqlTable);
        }
        
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath.toString()), StandardCharsets.UTF_8))) {
            writer.write("Tables Num: " + tables.size() + "\n");
            for (int i = 0; i < tables.size(); i++) {
                writer.write("Table " + i + ": " + tables.get(i).getTableName() + "\n");
                writer.write("Create SQL " + i + ": " + tables.get(i).getCreateSql() + "\n");
            }
            System.out.println("Text file created successfully: " + filePath);
        }
        catch (IOException e) {
            System.out.println("An error occurred.");
        }
    }

    public void LoadTables(String TablePath) throws Exception {
        // 1. 清空原有状态
        state.getSchema().clearTable();
        tables.clear();

        // 2. 验证文件有效性
        Path filePath = Paths.get(TablePath);
        if (!Files.exists(filePath)) {
            throw new IllegalArgumentException("指定的文件不存在: " + TablePath);
        }
        if (!Files.isRegularFile(filePath)) {
            throw new IllegalArgumentException("指定的路径不是有效文件: " + TablePath);
        }
        if (!TablePath.endsWith(".sql")) {
            throw new IllegalArgumentException("指定的文件不是.sql文件: " + TablePath);
        }

        // 3. 定义解析用的正则表达式
        java.util.regex.Pattern tableNumPattern = java.util.regex.Pattern.compile("Tables Num: (\\d+)");
        java.util.regex.Pattern tableNamePattern = java.util.regex.Pattern.compile("Table (\\d+): (.+)");
        java.util.regex.Pattern createSqlPattern = java.util.regex.Pattern.compile("Create SQL (\\d+): (.+)");
        
        int tableCount = 0;
        // 临时存储表信息：key=表索引，value=[表名, 创建SQL]
        Map<Integer, Object[]> tableInfoMap = new HashMap<>();

        // 4. 读取并解析文件内容
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath.toFile()), StandardCharsets.UTF_8))) {
            
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // 匹配表总数
                Matcher tableNumMatcher = tableNumPattern.matcher(line);
                if (tableNumMatcher.matches()) {
                    tableCount = Integer.parseInt(tableNumMatcher.group(1));
                    continue;
                }

                // 匹配表名
                Matcher tableNameMatcher = tableNamePattern.matcher(line);
                if (tableNameMatcher.matches()) {
                    int index = Integer.parseInt(tableNameMatcher.group(1));
                    String tableName = tableNameMatcher.group(2);
                    tableInfoMap.computeIfAbsent(index, k -> new Object[2])[0] = tableName;
                    continue;
                }

                // 匹配创建SQL
                Matcher createSqlMatcher = createSqlPattern.matcher(line);
                if (createSqlMatcher.matches()) {
                    int index = Integer.parseInt(createSqlMatcher.group(1));
                    String createSql = createSqlMatcher.group(2);
                    tableInfoMap.computeIfAbsent(index, k -> new Object[2])[1] = createSql;
                    continue;
                }
            }
        }

        // 5. 验证解析结果完整性
        if (tableCount <= 0) {
            throw new IOException("文件解析失败：未找到表数量信息");
        }
        if (tableInfoMap.size() != tableCount) {
            throw new IOException("解析文件失败：预期 " + tableCount + " 个表，实际解析出 " + tableInfoMap.size() + " 个");
        }

        // 6. 重建表对象并加载到内存
        for (int i = 0; i < tableCount; i++) {
            Object[] tableInfo = tableInfoMap.get(i);
            if (tableInfo == null || tableInfo[0] == null || tableInfo[1] == null) {
                throw new IOException("解析文件失败：缺少表 " + i + " 的名称或创建SQL");
            }
            
            String tableName = (String) tableInfo[0];
            String createSql = (String) tableInfo[1];
            
            // 创建TxnTable并设置属性
            TxnTable table = new TxnTable(state);
            table.setTableName(tableName);
            table.setCreateSql(createSql);
            tables.add(table);
            
            MySQLTable sqlTable = table.getMySQLTable();
            // 添加到列表和Schema
            state.getSchema().addTable(sqlTable);

            System.out.println(table.toString());
        }

        System.out.println("成功加载文件：" + TablePath);
        System.out.println("共加载 " + tables.size() + " 个表结构");

        Path dirPath = Paths.get(save_path);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        String fileName = "test_sample_" + System.currentTimeMillis() + Randomly.getNotCachedInteger(0, 10000) + ".txt";
        Path savePath = dirPath.resolve(fileName);

        Txn_constant.setIsolationLevel();
        
        // 生成测试用例
        Cases test_case = new Cases(state, tables, txn_num);
        test_case.generateCase();

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(savePath.toString()), StandardCharsets.UTF_8))) {
            writer.write("Tables Num: " + tables.size() + "\n");
            for (int i = 0; i < tables.size(); i++) {
                writer.write("Table " + i + ": " + tables.get(i).getTableName() + "\n");
                writer.write("Create SQL " + i + ": " + tables.get(i).getCreateSql() + "\n");
            }
            writer.write("Case Num: " + test_case.all_cases.size() + "\n");
            writer.write(test_case.toString());
            System.out.println("Text file created successfully: " + savePath);
        }
        catch (IOException e) {
            System.out.println("An error occurred.");
        }
    }

>>>>>>> e2d898d (添加APTrans核心代码)
    public void check() throws Exception {
        Path dirPath = Paths.get(save_path);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        String fileName = "test_sample_" + System.currentTimeMillis() + Randomly.getNotCachedInteger(0, 10000) + ".txt";
        Path filePath = dirPath.resolve(fileName);

        state.getSchema().clearTable();
        tables.clear();

        Txn_constant.setIsolationLevel();

        for (int i = 0; i < table_num; i++) {
            TxnTable table = new TxnTable(state);
            MySQLTable sqlTable = table.getMySQLTable();
            tables.add(table);
            state.getSchema().addTable(sqlTable);
        }

        Cases test_case = new Cases(state, tables, txn_num);
        test_case.generateCase();

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath.toString()), StandardCharsets.UTF_8))) {
            writer.write("Tables Num: " + tables.size() + "\n");
            for (int i = 0; i < tables.size(); i++) {
                writer.write("Table " + i + ": " + tables.get(i).getTableName() + "\n");
                writer.write("Create SQL " + i + ": " + tables.get(i).getCreateSql() + "\n");
            }
            writer.write("Case Num: " + test_case.all_cases.size() + "\n");
            writer.write(test_case.toString());
            System.out.println("Text file created successfully: " + filePath);
        }
        catch (IOException e) {
            System.out.println("An error occurred.");
        }
    }

<<<<<<< HEAD
=======
    public void generateSQL(int sql_nums) throws Exception {
        Path dirPath = Paths.get(save_path);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        String fileName = "test_sample_" + System.currentTimeMillis() + Randomly.getNotCachedInteger(0, 10000) + ".txt";
        Path filePath = dirPath.resolve(fileName);

        state.getSchema().clearTable();
        tables.clear();

        Txn_constant.setIsolationLevel();

        for (int i = 0; i < table_num; i++) {
            TxnTable table = new TxnTable(state);
            MySQLTable sqlTable = table.getMySQLTable();
            tables.add(table);
            state.getSchema().addTable(sqlTable);
        }

        Cases test_case = new Cases(state, tables, txn_num);
        test_case.generateSQL();

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath.toString()), StandardCharsets.UTF_8))) {
            writer.write("Tables Num: " + tables.size() + "\n");
            for (int i = 0; i < tables.size(); i++) {
                writer.write("Table " + i + ": " + tables.get(i).getTableName() + "\n");
                writer.write("Create SQL " + i + ": " + tables.get(i).getCreateSql() + "\n");
            }
            writer.write("SQL Num: " + sql_nums + "\n");
            writer.write(test_case.getSQLs(sql_nums));
            System.out.println("Text file created successfully: " + filePath);
        }
        catch (IOException e) {
            System.out.println("An error occurred.");
        }
    }

    public void LoadTables_SQL(String TablePath, int sql_nums) throws Exception {
        // 1. 清空原有状态
        state.getSchema().clearTable();
        tables.clear();

        // 2. 验证文件有效性
        Path filePath = Paths.get(TablePath);
        if (!Files.exists(filePath)) {
            throw new IllegalArgumentException("指定的文件不存在: " + TablePath);
        }
        if (!Files.isRegularFile(filePath)) {
            throw new IllegalArgumentException("指定的路径不是有效文件: " + TablePath);
        }
        if (!TablePath.endsWith(".sql")) {
            throw new IllegalArgumentException("指定的文件不是.sql文件: " + TablePath);
        }

        // 3. 定义解析用的正则表达式
        java.util.regex.Pattern tableNumPattern = java.util.regex.Pattern.compile("Tables Num: (\\d+)");
        java.util.regex.Pattern tableNamePattern = java.util.regex.Pattern.compile("Table (\\d+): (.+)");
        java.util.regex.Pattern createSqlPattern = java.util.regex.Pattern.compile("Create SQL (\\d+): (.+)");
        
        int tableCount = 0;
        // 临时存储表信息：key=表索引，value=[表名, 创建SQL]
        Map<Integer, Object[]> tableInfoMap = new HashMap<>();

        // 4. 读取并解析文件内容
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath.toFile()), StandardCharsets.UTF_8))) {
            
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // 匹配表总数
                Matcher tableNumMatcher = tableNumPattern.matcher(line);
                if (tableNumMatcher.matches()) {
                    tableCount = Integer.parseInt(tableNumMatcher.group(1));
                    continue;
                }

                // 匹配表名
                Matcher tableNameMatcher = tableNamePattern.matcher(line);
                if (tableNameMatcher.matches()) {
                    int index = Integer.parseInt(tableNameMatcher.group(1));
                    String tableName = tableNameMatcher.group(2);
                    tableInfoMap.computeIfAbsent(index, k -> new Object[2])[0] = tableName;
                    continue;
                }

                // 匹配创建SQL
                Matcher createSqlMatcher = createSqlPattern.matcher(line);
                if (createSqlMatcher.matches()) {
                    int index = Integer.parseInt(createSqlMatcher.group(1));
                    String createSql = createSqlMatcher.group(2);
                    tableInfoMap.computeIfAbsent(index, k -> new Object[2])[1] = createSql;
                    continue;
                }
            }
        }

        // 5. 验证解析结果完整性
        if (tableCount <= 0) {
            throw new IOException("文件解析失败：未找到表数量信息");
        }
        if (tableInfoMap.size() != tableCount) {
            throw new IOException("解析文件失败：预期 " + tableCount + " 个表，实际解析出 " + tableInfoMap.size() + " 个");
        }

        // 6. 重建表对象并加载到内存
        for (int i = 0; i < tableCount; i++) {
            Object[] tableInfo = tableInfoMap.get(i);
            if (tableInfo == null || tableInfo[0] == null || tableInfo[1] == null) {
                throw new IOException("解析文件失败：缺少表 " + i + " 的名称或创建SQL");
            }
            
            String tableName = (String) tableInfo[0];
            String createSql = (String) tableInfo[1];
            
            // 创建TxnTable并设置属性
            TxnTable table = new TxnTable(state);
            table.setTableName(tableName);
            table.setCreateSql(createSql);
            tables.add(table);
            
            MySQLTable sqlTable = table.getMySQLTable();
            // 添加到列表和Schema
            state.getSchema().addTable(sqlTable);

            System.out.println(table.toString());
        }

        System.out.println("成功加载文件：" + TablePath);
        System.out.println("共加载 " + tables.size() + " 个表结构");

        Path dirPath = Paths.get(save_path);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        String fileName = "test_sample_" + System.currentTimeMillis() + Randomly.getNotCachedInteger(0, 10000) + ".txt";
        Path savePath = dirPath.resolve(fileName);

        Txn_constant.setIsolationLevel();
        
        // 生成测试用例
        Cases test_case = new Cases(state, tables, txn_num);
        test_case.generateSQL();

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(savePath.toString()), StandardCharsets.UTF_8))) {
            writer.write("Tables Num: " + tables.size() + "\n");
            for (int i = 0; i < tables.size(); i++) {
                writer.write("Table " + i + ": " + tables.get(i).getTableName() + "\n");
                writer.write("Create SQL " + i + ": " + tables.get(i).getCreateSql() + "\n");
            }
            writer.write("SQL Num: " + sql_nums + "\n");
            writer.write(test_case.getSQLs(sql_nums));
            System.out.println("Text file created successfully: " + filePath);
        }
        catch (IOException e) {
            System.out.println("An error occurred.");
        }
    }

>>>>>>> e2d898d (添加APTrans核心代码)
}