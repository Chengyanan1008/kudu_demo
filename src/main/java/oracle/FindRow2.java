package oracle;

import java.sql.*;

/**
 * 按条件搜索
 * @author Administrator
 *
 */
public class FindRow2 {
    public static void main(String[] args) throws  Exception {

        Connection ct = DB.getConnection();
        PreparedStatement pstm ;
        ResultSet rs;

        String sql = "select * from student where 1 = 1";
        int count = 0;
        try {
            pstm = ct.prepareStatement(sql);
            rs = pstm.executeQuery();
            while (rs.next()) {
                count++;
            }

            ResultSetMetaData rsmd = rs.getMetaData();
            int cols_len = rsmd.getColumnCount();

            System.out.println("count=" + count + "\tcols_len=" + cols_len);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ct.close();
        }

    }
}