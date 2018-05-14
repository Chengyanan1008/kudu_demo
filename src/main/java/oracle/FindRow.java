package oracle;

import org.apache.kudu.client.*;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 按条件搜索
 * @author Administrator
 *
 */
public class FindRow {
    public static void main(String[] args) throws  Exception {

        Connection ct = DB.getConnection();
        PreparedStatement pstm ;
        ResultSet rs;

        String sql = "select * from student where 1 = 1";
        try {
            pstm = ct.prepareStatement(sql);
            rs = pstm.executeQuery();
            while (rs.next()) {
                String id = rs.getString("id");
                String name = rs.getString("stu_name");
                String gender = rs.getString("gender");
                String age = rs.getString("age");
                String address = rs.getString("address");
                System.out.println(id + "\t" + name + "\t" + gender + "\t"
                        + age + "\t" + address);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
           ct.close();
        }

    }
}