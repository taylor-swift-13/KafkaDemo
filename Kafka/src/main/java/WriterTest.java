
import com.opencsv.CSVWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriterTest {
    public static void main(String[] args) {
        String str="{" +
                "  \"eventDate\":\"2021-01-01\"," +
                "  \"eventType\":\"shop\"," +
                "  \"eventBody\":{" +
                "    \"tran_date\":\"2021-01-01\"," +
                "    \"tran_channel\":\"二维码交易\"," +
                "    \"shop_code\":\"DFRCB_S_5369167466021584\"," +
                "    \"hlw_tran_type\":\"ZFB\"," +
                "    \"tran_time\":\"21:12:06\"," +
                "    \"shop_name\":\"商户_陈洁洁\"," +
                "    \"order_code\":\"DFRCB_ZP_1609506726646XGV67C\"," +
                "    \"uid\":\"320926197808312621\"," +
                "    \"score_num\":\"0\"," +
                "    \"current_status\":\"24\"," +
                "    \"tran_amt\":\"20.40\"," +
                "    \"legal_name\":\"贺巷巷\"," +
                "    \"etl_dt\":\"2021-01-01\"" +
                "  }" +
                "}";
        for(int i=0;i<10;i++){
            String fileName="F:\\kafkaTest\\test.txt";
            writeTXT(fileName,str);
        }
    }

    public static void writeTXT(String fileName,String content){
        try {
            // 防止文件建立或读取失败，用catch捕捉错误并打印，也可以throw
            /* 写入Txt文件 */
            File file= new File(fileName);// 相对路径，如果没有则要建立一个新的output。txt文件
            BufferedWriter out = new BufferedWriter(new FileWriter(file,true));
            out.write(content); // \r\n即为换行
            out.write("\r\n");
            out.flush(); // 把缓存区内容压入文件
            out.close(); // 最后记得关闭文件
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
