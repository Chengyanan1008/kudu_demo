package oracle_to_kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;


/**
 * Created by qls on 18-5-14.
 */
public class OracleToKudu {

    private static String USERNAMR = "system";
    private static String PASSWORD = "oracle";
    private static String DRVIER = "oracle.jdbc.OracleDriver";
    private static String URL = "jdbc:oracle:thin:@localhost:1521:xe";
    private static String KUDU_TABLE = "student";
    private  static  String KUDU_MASTER_ADDRESS = "localhost";

    private static ColumnSchema newKuduColumn(String name, Type type, boolean iskey) {
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        column.key(iskey);
        return column.build();
    }

    public static Connection getOracleConnection(){

        Connection connection = null;
        try {
            Class.forName(DRVIER);
            connection = DriverManager.getConnection(URL, USERNAMR, PASSWORD);
            System.out.println("成功连接数据库");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("class not find !", e);
        } catch (SQLException e) {
            throw new RuntimeException("get connection error!", e);
        }

        return connection;

    }

    private static KuduClient initKuduClient() {

        // 创建kudu的数据库链接
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER_ADDRESS).defaultSocketReadTimeoutMs(6000).build();

        return  client;
    }

    private static void createKuduTable(String tableName, KuduClient client) {


        // 设置表的schema
        List<ColumnSchema> columns = new LinkedList<ColumnSchema>();
        columns.add(newKuduColumn("id", Type.INT32, true));
        columns.add(newKuduColumn("stu_name", Type.STRING, false));
        columns.add(newKuduColumn("gender", Type.INT32, false));
        columns.add(newKuduColumn("age", Type.INT32, false));
        columns.add(newKuduColumn("address", Type.STRING, false));
        Schema schema = new Schema(columns);
        //创建表时提供的所有选项
        CreateTableOptions options = new CreateTableOptions();
        // 设置表的replica备份和分区规则
        List<String> parcols = new LinkedList<String>();
        parcols.add("id");

        // 一个replica
        options.setNumReplicas(1);
        // 用列companyid做为分区的参照
        options.setRangePartitionColumns(parcols);
        // 添加key的hash分区
        options.addHashPartitions(parcols, 3);
        try {
            client.createTable(tableName, schema, options);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws  Exception {

        //1. create kudu table
        KuduClient kc = initKuduClient();

        if( ! kc.tableExists(KUDU_TABLE)){
            createKuduTable(KUDU_TABLE, kc);
        }

        KuduTable table = kc.openTable(KUDU_TABLE);
        // 创建写session,kudu必须通过session写入
        KuduSession session = kc.newSession();
        // 采取Flush方式 手动刷新
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(3000);

        //2. read oracle data
        Connection ct = getOracleConnection();
        PreparedStatement pstm ;
        ResultSet rs;

        String sql = "select * from student where 1 = 1";
        try {
            pstm = ct.prepareStatement(sql);
            rs = pstm.executeQuery();
            while (rs.next()) {

                //3. insert data to kudu
                insertIntoKudu(rs,session, table);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ct.close();
            session.close();
            kc.close();
        }

    }

    private static void insertIntoKudu(ResultSet rs, KuduSession session, KuduTable table) throws  Exception{
        String id = rs.getString("id");
        String name = rs.getString("stu_name");
        String gender = rs.getString("gender");
        String age = rs.getString("age");
        String address = rs.getString("address");

        Insert insert = table.newInsert();
        insert.getRow().addInt("id", Integer.valueOf(id));
        insert.getRow().addString("stu_name", name);
        insert.getRow().addInt("gender", Integer.valueOf(gender));
        insert.getRow().addInt("age", Integer.valueOf(age));
        insert.getRow().addString("address", address);
        session.flush();
        session.apply(insert);

    }


}
