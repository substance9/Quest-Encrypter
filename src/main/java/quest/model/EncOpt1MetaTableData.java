package quest.model;

public class EncOpt1MetaTableData {
    public String encD;
    public String encCount;

    public EncOpt1MetaTableData(String encD, String encCount){
        this.encD = encD;
        this.encCount = encCount;
    }

    @Override
    public String toString()
    {
        return "Enc Opt1 Meta Table Data: " + " - encD: "  + encD + " - encCount: "  + encCount;
    }
}

