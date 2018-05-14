package kudu_to_oracle;

import org.apache.kudu.client.*;

import java.sql.*;


/**
 * Created by qls on 18-5-14.
 */
public class KuduToOracle {

    private static String USERNAMR = "system";
    private static String PASSWORD = "oracle";
    private static String DRVIER = "oracle.jdbc.OracleDriver";
    private static String URL = "jdbc:oracle:thin:@localhost:1521:xe";
    private static String KUDU_TABLE = "student";
    private  static  String KUDU_MASTER_ADDRESS = "localhost";

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


    public static void main(String[] args) throws  Exception {

        //1. create oracle table

        Connection ct = getOracleConnection();
        String sql = "create table student2\n" +
                "(\n" +
                "       id number(11) not null primary key,\n" +
                "       stu_name varchar(16) not null,\n" +
                "       gender number(11) default null,\n" +
                "       age number(11) default null,\n" +
                "       address varchar(128) default null\n" +
                ")";

        Statement st = ct.createStatement();
        st.execute("drop table student2");
        st.execute(sql);
        st.close();


        //2. init kudu client
        KuduClient kc = initKuduClient();
        KuduTable table = kc.openTable(KUDU_TABLE);
        KuduScanner.KuduScannerBuilder builder = kc.newScannerBuilder(table);

        // 3. read kudu data
        KuduScanner scaner = builder.build();
        while (scaner.hasMoreRows()) {
            RowResultIterator iterator = scaner.nextRows();
            while (iterator.hasNext()) {
                RowResult result = iterator.next();

                //4. insert data to oracle
                insertIntoOrcle(result, ct);

            }
        }

        scaner.close();
        kc.close();
        ct.close();

    }

    private static void insertIntoOrcle(RowResult result, Connection ct) {


        int id = result.getInt("id");
        String name = result.getString("stu_name");
        int gender = result.getInt("gender");
        int age = result.getInt("age");
        String address = result.getString("address");

        String sqlStr = "insert into student2 values(?,?,?,?,?)";
        PreparedStatement pstm ;
        try {

            pstm = ct.prepareStatement(sqlStr);
            pstm.setInt(1, id);
            pstm.setString(2, name);
            pstm.setInt(3, gender);
            pstm.setInt(4, age);
            pstm.setString(5, address);
            pstm.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}