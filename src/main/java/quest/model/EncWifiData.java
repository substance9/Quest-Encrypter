package quest.model;

public class EncWifiData {
    public String encId;
    public String encU;
    public String encL;
    public String encCL;
    public String encD;

    public EncWifiData(){
        this.encId = null;
        this.encU = null;
        this.encL = null;
        this.encCL = null;
        this.encD = null;
    }

    public EncWifiData(String encId, String encU, String encL, String encCL, String encD){
        this.encId = encId;
        this.encU = encU;
        this.encL = encL;
        this.encCL = encCL;
        this.encD = encD;
    }

    @Override
    public String toString()
    {
        return "Enc Wifi Data: " + " - encId: " + encId + " - encU: "  + encU + " - encL: "  + encL + " - encCL: "  + encCL + " - encD: "  + encD;
    }
}

