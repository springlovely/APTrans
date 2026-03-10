package sqlancer;

import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;

public abstract class GlobalState<O extends DBMSSpecificOptions<?>, S extends AbstractSchema<?, ?>, C extends Object> {

    private Randomly r;
    private S schema;
//    private StateToReproduce state;
    private String databaseName;
<<<<<<< HEAD

//    public void setConnection(C con) {
//        this.databaseConnection = con;
//    }

    public C getConnection() {
//        return databaseConnection;
=======
    private MainOptions options;

    public C getConnection() {
>>>>>>> e2d898d (添加APTrans核心代码)
        return null;
    }

    public void setRandomly(Randomly r) {
        this.r = r;
    }

<<<<<<< HEAD
=======
    public MainOptions getOptions() {
        return options;
    }

>>>>>>> e2d898d (添加APTrans核心代码)
    public Randomly getRandomly() {
        return r;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public S getSchema() {
        if (schema == null) {
            try {
                updateSchema();
            } catch (Exception e) {
                throw new AssertionError(e.getMessage());
            }
        }
        return schema;
    }

<<<<<<< HEAD
    protected void setSchema(S schema) {
        this.schema = schema;
    }

=======
    public void setSchema(S schema) {
        this.schema = schema;
    }

    public void setMainOptions(MainOptions options) {
        this.options = options;
    }

>>>>>>> e2d898d (添加APTrans核心代码)
    public void updateSchema() throws Exception {
        setSchema(readSchema());
        for (AbstractTable<?, ?, ?> table : schema.getDatabaseTables()) {
            table.recomputeCount();
        }
    }

    protected abstract S readSchema() throws Exception;

}
