package kudu;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

public class DropTable {

    public static void main(String[] args) throws KuduException {
        // master地址
        final String masteraddr = "localhost";
        // 创建kudu的数据库链接
        KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(6000).build();

        client.deleteTable("PERSON");
        client.close();
    }

}
