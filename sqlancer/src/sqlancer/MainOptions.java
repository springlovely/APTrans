package sqlancer;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
@Parameters(separators = "=", commandDescription = "Options for Transaction Generation")
public class MainOptions {
    @Parameter(names = "--sample_type", description = "the type of samples to generate, include: mysql, postgres or all")
    private static String sample_type = "all";

    @Parameter(names = "--sample_num", description = "total number of samples to generate")
    private static int sample_num = 100;

    @Parameter(names = "--save_path", description = "the path to save the generated cases")
    private static String save_path = "../cases/";

    @Parameter(names = "--clean_save_path", description = "the path to save the generated cases")
    private static String clean_save_path = "true";

    @Parameter(names = "--table_num", description = "the number of tables to generate in each case")
    private static int table_num = 1;

    @Parameter(names = "--random_table_num", description = "the number of tables to generate in each case")
    private static String random_table_num = "false";

    @Parameter(names = "--txn_num", description = "the number of transactions to generate in each case")
<<<<<<< HEAD
    private static int txn_num = 128;
=======
    private static int txn_num = 64;
>>>>>>> e2d898d (添加APTrans核心代码)
 
    @Parameter(names = "--test_isolation", description = "the database isolation to test [serializable, repeatable_read, read_committed]")
    private static String test_isolation  = "serializable";

<<<<<<< HEAD
=======
    @Parameter(names = "--only_generate_database", description = "whether to only generate database structure (without transactions, true/false)")
    private static String only_generate_database = "false";

    @Parameter(names = "--database_path", description = "database structure path")
    private static String database_path = "./databases";

    @Parameter(names = "--load_database", description = "whether to load existing database structure from save_path (true/false)")
    private static String load_database = "false";

    @Parameter(names = "--single_sql", description = "whether to generate only a single SQL statement (true/false)")
    private static String single_sql = "false";

    @Parameter(names = "--sqls_num", description = "the number of sqls to generate")
    private static int sqls_num = 64;

    @Parameter(names = "--depth", description = "the depth of expression")
    private static int depth = 3;

    public int getMaxExpressionDepth(){
        return depth;
    }

    public static boolean isOnlySQL() {
        return "true".equalsIgnoreCase(single_sql);
    }

    public int getSQLs_num() {
        return sqls_num;
    }

    public static boolean isOnlyGenerateDatabase() {
        return "true".equalsIgnoreCase(only_generate_database);
    }

    public static boolean isLoadDatabase() {
        return "true".equalsIgnoreCase(load_database);
    }

    public String getDatabasePath() {
        return database_path;
    }

>>>>>>> e2d898d (添加APTrans核心代码)
    public String getSample_type(){
        return sample_type;
    }

    public String getSave_path(){
        return save_path;
    }

    public boolean getClean_save_path(){
        return clean_save_path.equals("true");
    }

    public int getSample_num(){
        return sample_num;
    }

    public int getTable_num(){
        if (random_table_num.equals("true")){
            return new Randomly().getInteger(1, 4);
        } else {
            return table_num;
        }
    }

    public int getTxn_num(){
        return txn_num;
    }

    public String getTest_isolation(){
        return test_isolation;
    }

}