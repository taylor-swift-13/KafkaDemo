package PO;

import com.alibaba.fastjson.JSONObject;

import java.util.Map;
import java.util.Set;

public class SdkData {
    public static String HOST = "114.212.241.8";
    private String eventType;
    private String eventBody;

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getEventBody() {
        return eventBody;
    }

    public void setEventBody(String eventBody) {
        this.eventBody = eventBody;
    }

    public static String resolveInsertSql(String eventType, Set<Map.Entry<String,Object>> set){
        StringBuffer sb = new StringBuffer();
        StringBuffer keys = new StringBuffer();
        sb.append("INSERT INTO dm.dm_v_tr_").append(eventType).append("_mx (*) VALUES")
                .append("(");
        for(Map.Entry<String,Object> entry: set){
            keys.append(entry.getKey()).append(",");
            sb.append('\'').append(entry.getValue().toString().replace("\'","")).append("\',");
        }
        keys.deleteCharAt(keys.length()-1);
        sb.deleteCharAt(sb.length()-1);
        sb.append(")");

        int target =  sb.indexOf("*");
        sb.deleteCharAt(target);
        sb.insert(target,keys.toString());
        return sb.toString();
    }

    public String resolveSql(){
        // 清洗
        JSONObject object = JSONObject.parseObject(eventBody);

        switch (eventType){
            case "sa":
                Object key =  object.get("tran_date_date");
                object.remove("tran_date_date");
                object.put("tran_date",key);
                break;
            default:
                break;
        }

        return resolveInsertSql(eventType,object.entrySet());
    }
}

