package quest.model;

public class EncOpt2MetaTableData {
    public String encD;
    public String encL;
    public String encCount;

    public EncOpt2MetaTableData(String encD, String encL, String encCount){
        this.encD = encD;
        this.encL = encL;
        this.encCount = encCount;
    }

    @Override
    public String toString()
    {
        return "Enc Opt2 Meta Table Data: " + " - encD: "  + encD + " - encL: "  + encL + " - encCount: "  + encCount;
    }
}

