package oracle;

import java.sql.Connection;
import java.sql.Statement;

public class DropTable {

    public static void main(String[] args) throws Exception {

        Connection ct = DB.getConnection();

        String sql = "drop table student";

        Statement st = ct.createStatement();
        st.execute(sql);
        st.close();
        ct.close();

    }

}
