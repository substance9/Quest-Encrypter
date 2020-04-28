package quest.model;

import java.sql.Timestamp;

public class RawWifiData {
    public Timestamp time;
    public String apId;
    public String clientId;

    public RawWifiData(Timestamp dataTimeTs, String apIdStr, String clientIdStr){
        time = dataTimeTs;
        apId = apIdStr;
        clientId = clientIdStr;
    }

    public int getSize(){
        return (8 + getStrSize(apId) + getStrSize(clientId));
    }

    public int getStrSize(String str){
        return (8 * (int) ((((str.length()) * 2) + 45) / 8)) - 8;
    }

    @Override
    public String toString()
    {
        return "Raw Wifi Data: " + " - Timestamp: " + time.toLocalDateTime() + " - apId: "  + apId + " - clientId: "  + clientId;
    }
}

