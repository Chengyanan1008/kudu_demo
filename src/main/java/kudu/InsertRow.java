package kudu;

import org.apache.kudu.client.*;
import org.apache.kudu.client.SessionConfiguration.FlushMode;

/**
 * kudu的插入
 * @author Administrator
 *
 */
public class InsertRow {
    public static void main(String[] args) throws KuduException {

        // master地址
        final String masteraddr = "localhost";
        // 创建kudu的数据库链接
        KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).build();
        // 打开表
        KuduTable table = client.openTable("PERSON");
        // 创建写session,kudu必须通过session写入
        KuduSession session = client.newSession();
        // 采取Flush方式 手动刷新
        session.setFlushMode(FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(3000);
        for (int i = 1; i < 100; i++) {
            Insert insert = table.newInsert();
            // 设置字段内容
            insert.getRow().addInt("CompanyId", i);
            insert.getRow().addInt("WorkId", i);
            insert.getRow().addString("Name", "lisi" + i);
            insert.getRow().addString("Gender", "male");
            insert.getRow().addString("Desc", "desc of the person"+ i);
            insert.getRow().addString("Photo", "person" + i);
            session.flush();
            session.apply(insert);
        }
        session.close();
        client.close();
    }
}
