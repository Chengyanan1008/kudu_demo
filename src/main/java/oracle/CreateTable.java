package oracle;


import java.sql.Connection;
import java.sql.Statement;

public class CreateTable {



    public static void main(String[] args) throws  Exception {

        Connection ct = DB.getConnection();

        String sql = "create table student\n" +
                "(\n" +
                "       id number(11) not null primary key,\n" +
                "       stu_name varchar(16) not null,\n" +
                "       gender number(11) default null,\n" +
                "       age number(11) default null,\n" +
                "       address varchar(128) default null\n" +
                ")";

        Statement st = ct.createStatement();
        st.execute(sql);
        st.close();
        ct.close();

    }

}
