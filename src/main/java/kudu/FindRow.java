package kudu;

import org.apache.kudu.client.*;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;

/**
 * 按条件搜索
 * @author Administrator
 *
 */
public class FindRow {
    public static void main(String[] args) throws KuduException {

        //master地址
        final String masteraddr = "localhost";

        //创建kudu的数据库链接
        KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(6000).build();

        //打开表
        KuduTable table = client.openTable("PERSON");

        /**
         * 设置搜索的条件
         */
        KuduScannerBuilder builder = client.newScannerBuilder(table);
//        //newComparisonPredicate 在一个整数或时间戳列上创建一个新的比较谓词。
//        KuduPredicate predicate = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn("CompanyId"),
//                ComparisonOp.EQUAL, 1);
//        builder.addPredicate(predicate);


        // 开始扫描
        KuduScanner scaner = builder.build();
        while (scaner.hasMoreRows()) {
            RowResultIterator iterator = scaner.nextRows();
            while (iterator.hasNext()) {
                RowResult result = iterator.next();
                /**
                 * 输出行
                 */
                System.out.println("CompanyId:" + result.getInt("CompanyId"));
                System.out.println("Name:" + result.getString("Name"));
                System.out.println("Gender:" + result.getString("Gender"));
                System.out.println("Desc:" + result.getString("Desc"));
            }
        }

        scaner.close();
        client.close();
    }
}