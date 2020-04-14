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

    @Override
    public String toString()
    {
        return "Raw Wifi Data: " + " - Timestamp: " + time.toLocalDateTime() + " - apId: "  + apId + " - clientId: "  + clientId;
    }
}

