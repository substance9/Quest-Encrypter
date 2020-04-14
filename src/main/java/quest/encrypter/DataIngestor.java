package quest.encrypter;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import quest.model.EncWifiData;

public class DataIngestor {
    public HikariDataSource dataDBConnectionPool;
    private Connection dataDbCon = null;

    private String inserSQLTemplate = "INSERT INTO %s (ENCID, ENCU, ENCL, ENCCL, ENCD) VALUES (?,?,?,?,?)";
    private String insertSQL;

    private long numAllRowsAffected;

    public DataIngestor(int dbPort, String encTableName) {
        Properties prop = getHikariDbProperties();

        String jdbcUrlBase = prop.getProperty("jdbcUrl");
        String dataJdbcUrl = jdbcUrlBase + ":" + String.valueOf(dbPort) + "/tippers_quest";

        insertSQL = String.format(inserSQLTemplate, encTableName);

        System.out.println("Preparing to connect to DB for data storage: " + dataJdbcUrl);

        // Data DB COnnection
        HikariConfig dataDbCfg = new HikariConfig(prop);
        dataDbCfg.setJdbcUrl(dataJdbcUrl);
        dataDbCfg.setMaximumPoolSize(2); // no need for MPL
        dataDbCfg.setAutoCommit(false);
        dataDBConnectionPool = new HikariDataSource(dataDbCfg);

        // try {
        //     dataDbCon = dataDBConnectionPool.getConnection();
        // } catch (SQLException e) {
        //     e.printStackTrace();
        //     return;
        // }

        numAllRowsAffected = 0;
    }

    public Properties getHikariDbProperties() {
        Properties prop = null;
        try (InputStream input = DataIngestor.class.getClassLoader().getResourceAsStream("postgres.properties")) {
            prop = new Properties();
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }

    public void ingestBatchToDB(ArrayList<EncWifiData> currBatchEncDataList){
        try{
            dataDbCon = dataDBConnectionPool.getConnection();
            dataDbCon.setAutoCommit(false);
            int numAllRowsAffectedInBatch = 0;

            for(EncWifiData eData:currBatchEncDataList){
                PreparedStatement pst = dataDbCon.prepareStatement(insertSQL);
                pst.setString(1, eData.encId);
                pst.setString(2, eData.encU);
                pst.setString(3, eData.encL);
                pst.setString(4, eData.encCL);
                pst.setString(5, eData.encD);
                int numRowsAffected = pst.executeUpdate();
                numAllRowsAffectedInBatch = numAllRowsAffectedInBatch + numRowsAffected;
            }
                
            dataDbCon.commit();
            System.out.println(String.valueOf(numAllRowsAffectedInBatch) + " Rows inserted for the batch"); 
            numAllRowsAffected = numAllRowsAffected + numAllRowsAffectedInBatch;
            System.out.println(String.valueOf(numAllRowsAffected) + " Rows inserted in Total"); 
            
        } catch (SQLException ex){
            ex.printStackTrace();
        }finally {
            try {
                if (dataDbCon != null) {
                    dataDbCon.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
}
