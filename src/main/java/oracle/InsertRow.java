package oracle;

import org.apache.kudu.client.*;
import org.apache.kudu.client.SessionConfiguration.FlushMode;

import java.sql.*;

/**
 * kudu的插入
 * @author Administrator
 *
 */
public class InsertRow {

    public static void main(String[] args) throws  Exception {

        Connection ct = DB.getConnection();
        // String sql =
        // "insert into student values('1','王小军','1','17','北京市和平里七区30号楼7门102')";
        String sql = "select count(*) from student where 1 = 1";
        String sqlStr = "insert into student values(?,?,?,?,?)";
        int count = 0;
        PreparedStatement pstm ;
        ResultSet rs;
        try {
            // 计算数据库student表中数据总数
            pstm = ct.prepareStatement(sql);
            rs = pstm.executeQuery();
            while (rs.next()) {
                count = rs.getInt(1) + 1;
                System.out.println(rs.getInt(1));
            }
            // 执行插入数据操作
            pstm = ct.prepareStatement(sqlStr);
            pstm.setInt(1, count);
            pstm.setString(2, "xxx");
            pstm.setInt(3, 14);
            pstm.setInt(4, 13);
            pstm.setString(5, "yyy");
            pstm.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ct.close();
        }

    }

}
