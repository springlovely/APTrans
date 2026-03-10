package sqlancer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
<<<<<<< HEAD
import java.util.stream.Collectors;
=======
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
>>>>>>> e2d898d (添加APTrans核心代码)

import com.beust.jcommander.JCommander;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.oracle.MySQLTxnGen;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.oracle.PostgresTxnGen;

public final class Main {
    static MainOptions options = new MainOptions();

<<<<<<< HEAD
    public static void MySQL_Case(String save_path) {
        if (options.getClean_save_path()) // Clean the save path
            ClearDir(save_path);
=======
    public static void MySQL_Case(String save_path) throws Exception { // 添加了 throws Exception
        if (options.getClean_save_path()) {
            ClearDir(save_path);
        }

>>>>>>> e2d898d (添加APTrans核心代码)
        MySQLGlobalState globalState = new MySQLGlobalState();
        Randomly r = new Randomly();
        globalState.setSchema(new MySQLSchema(new ArrayList<>()));
        globalState.setRandomly(r);
<<<<<<< HEAD

        try {
=======
        globalState.setMainOptions(options);

        if (MainOptions.isOnlyGenerateDatabase()) {
            for (int i = 0; i < options.getSample_num(); i++) {
                MySQLTxnGen txnGen = new MySQLTxnGen(globalState, options.getTable_num(), options.getTxn_num(), save_path);
                txnGen.MySQLDatabaseGenerate(save_path);
            }
            return;
        } 
        if (MainOptions.isOnlySQL()){
            if (MainOptions.isLoadDatabase()) {
                Path dirPath = Paths.get(options.getDatabasePath());
                
                if (!Files.exists(dirPath) || !Files.isDirectory(dirPath)) {
                    throw new IllegalArgumentException("目录不存在或不是有效目录: " + dirPath);
                }
    
                // 使用 try-with-resources 自动关闭文件流
                try (Stream<Path> stream = Files.list(dirPath)) {
                    List<Path> sqlFiles = stream
                        .filter(Files::isRegularFile) // 简写形式
                        .filter(path -> {
                            String fileName = path.getFileName().toString();
                            return fileName.startsWith("Database_") && fileName.endsWith(".sql");
                        })
                        .collect(Collectors.toList());
    
                    for (Path file : sqlFiles) {
                        MySQLTxnGen txnGen = new MySQLTxnGen(globalState, options.getTable_num(), options.getSQLs_num(), save_path);
                        // 直接传递绝对路径字符串
                        txnGen.LoadTables_SQL(file.toAbsolutePath().toString(), options.getSQLs_num());
                    }
                } 
            } 
            else {
                for (int i = 0; i < options.getSample_num(); i++) {
                    MySQLTxnGen txnGen = new MySQLTxnGen(globalState, options.getTable_num(), options.getSQLs_num(), save_path);
                    txnGen.generateSQL(options.getSQLs_num());
                }
            }
            return;
        }
        if (MainOptions.isLoadDatabase()) {
            Path dirPath = Paths.get(options.getDatabasePath());
            
            if (!Files.exists(dirPath) || !Files.isDirectory(dirPath)) {
                throw new IllegalArgumentException("目录不存在或不是有效目录: " + dirPath);
            }

            // 使用 try-with-resources 自动关闭文件流
            try (Stream<Path> stream = Files.list(dirPath)) {
                List<Path> sqlFiles = stream
                    .filter(Files::isRegularFile) // 简写形式
                    .filter(path -> {
                        String fileName = path.getFileName().toString();
                        return fileName.startsWith("Database_") && fileName.endsWith(".sql");
                    })
                    .collect(Collectors.toList());

                for (Path file : sqlFiles) {
                    MySQLTxnGen txnGen = new MySQLTxnGen(globalState, options.getTable_num(), options.getTxn_num(), save_path);
                    // 直接传递绝对路径字符串
                    txnGen.LoadTables(file.toAbsolutePath().toString());
                }
            } 
        } 
        else {
>>>>>>> e2d898d (添加APTrans核心代码)
            for (int i = 0; i < options.getSample_num(); i++) {
                MySQLTxnGen txnGen = new MySQLTxnGen(globalState, options.getTable_num(), options.getTxn_num(), save_path);
                txnGen.check();
            }
<<<<<<< HEAD
        } catch (Exception e) {
            e.printStackTrace();
=======
>>>>>>> e2d898d (添加APTrans核心代码)
        }
    }

    public static void Postgres_Case(String save_path) {
        if (options.getClean_save_path()) // Clean the save path
            ClearDir(save_path);
        PostgresGlobalState globalState = new PostgresGlobalState();
        Randomly r = new Randomly();
        globalState.setSchema(new PostgresSchema(new ArrayList<>()));
        globalState.setRandomly(r);

        try {
            for (int i = 0; i < options.getSample_num(); i++) {
                PostgresTxnGen txnGen = new PostgresTxnGen(globalState, options.getTable_num(), options.getTxn_num(), save_path);
                txnGen.check();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void ClearDir(String save_path) {
        Path dirPath = Paths.get(save_path);
        if (Files.exists(dirPath)) {
            System.out.println("Cleaning the save path: " + save_path);
            try {
                Files.walk(dirPath).sorted(Comparator.reverseOrder()).map(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                System.err.println("Failed to delete " + path + ": " + e.getMessage());
                            }
                            return path;
                        }).collect(Collectors.toList()); // 收集结果（可选）
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

<<<<<<< HEAD
    public static void main(String[] args) {
        JCommander jCommander = new JCommander(options, args);
        jCommander.setProgramName("Transaction_Producer");

=======
    public static void main(String[] args) throws Exception { // 在这里添加 throws Exception
        JCommander jCommander = new JCommander(options, args);
        jCommander.setProgramName("Transaction_Producer");
    
>>>>>>> e2d898d (添加APTrans核心代码)
        if (options.getSample_type().equals("mysql")) {
            MySQL_Case(options.getSave_path());
        } else if (options.getSample_type().equals("postgres")) {
            Postgres_Case(options.getSave_path());
        } else if (options.getSample_type().equals("all")) {
            MySQL_Case(options.getSave_path());
<<<<<<< HEAD
            Postgres_Case( options.getSave_path());
=======
            Postgres_Case(options.getSave_path());
>>>>>>> e2d898d (添加APTrans核心代码)
        } else {
            System.out.println("Invalid case type");
        }
    }
}
