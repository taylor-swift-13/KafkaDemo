import PO.SdkData;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class TransformTest {
    public static void main(String[] args) {
        ArrayList<String> values=new ArrayList<>();
         try {
             String fileName = "F:\\kafkaTest\\special.txt";
             BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
             String value = null;
             while ((value = bufferedReader.readLine())!=null) {
                 values.add(value);
                 System.out.println(value);
             }
         }catch(Exception e) {
             e.printStackTrace();
         }

        String input = values.get(1);
        SdkData sdkData=null;
           try{
               sdkData = JSON.parseObject(input, SdkData.class);
               System.out.println("body:"+sdkData.getEventBody());
            }catch (Exception e){
                e.printStackTrace();
                System.out.println("err:"+input);
                System.out.println("body:"+sdkData.getEventBody());
            }


    }
}
