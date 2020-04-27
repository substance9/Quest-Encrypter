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

import quest.model.EncOpt1MetaTableData;
import quest.model.EncOpt2MetaTableData;
import quest.model.EncWifiData;

public class DataIngestor {
    public HikariDataSource dataDBConnectionPool;
    private Connection dataDbCon = null;

    private String inserOpt1MetaTableSQLTemplate = "INSERT INTO %s (ENCD, ENCCOUNT) VALUES (?,?)";
    private String inserOpt2MetaTableSQLTemplate = "INSERT INTO %s (ENCD, ENCL, ENCCOUNT) VALUES (?,?,?)";
    private String inserSQLTemplate = "INSERT INTO %s (ENCID, ENCU, ENCL, ENCCL, ENCD) VALUES (?,?,?,?,?)";
    private String insertSQL;
    private String inserOpt1MetaTableSQL;
    private String inserOpt2MetaTableSQL;


    private long numAllRowsAffected;

    public DataIngestor(int dbPort, String encTableName) {
        Properties prop = getHikariDbProperties();

        String jdbcUrlBase = prop.getProperty("jdbcUrl");
        String dataJdbcUrl = jdbcUrlBase + ":" + String.valueOf(dbPort) + "/tippers_quest";

        insertSQL = String.format(inserSQLTemplate, encTableName);
        inserOpt1MetaTableSQL = String.format(inserOpt1MetaTableSQLTemplate, encTableName+"_meta_opt1");
        inserOpt2MetaTableSQL = String.format(inserOpt2MetaTableSQLTemplate, encTableName+"_meta_opt2");

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

    //Ingest function for all optimization method enabled
    public void ingestBatchToDB(ArrayList<EncWifiData> currBatchEncDataList, EncOpt1MetaTableData encOpt1MetaData, ArrayList<EncOpt2MetaTableData> encOpt2MetaDataList){
        ingestBatchToDB(currBatchEncDataList);
        ingestOpt1MetaDataToDB(encOpt1MetaData);
        ingestOpt2MetaDataToDB(encOpt2MetaDataList);
    }

    //Ingest function for optimization method 1
    public void ingestBatchToDB(ArrayList<EncWifiData> currBatchEncDataList, EncOpt1MetaTableData encOpt1MetaData){
        ingestBatchToDB(currBatchEncDataList);

        ingestOpt1MetaDataToDB(encOpt1MetaData);
    }

    //Ingest function for optimization method 2
    public void ingestBatchToDB(ArrayList<EncWifiData> currBatchEncDataList, ArrayList<EncOpt2MetaTableData> encOpt2MetaDataList){
        ingestBatchToDB(currBatchEncDataList);

        ingestOpt2MetaDataToDB(encOpt2MetaDataList);
    }

    public void ingestOpt1MetaDataToDB(EncOpt1MetaTableData encOpt1MetaData){
        try{
            dataDbCon = dataDBConnectionPool.getConnection();
            dataDbCon.setAutoCommit(true);

            PreparedStatement pst = dataDbCon.prepareStatement(inserOpt1MetaTableSQL);
            pst.setString(1, encOpt1MetaData.encD);
            pst.setString(2, encOpt1MetaData.encCount);
            
            int numRowsAffected = pst.executeUpdate();

            System.out.println("Meta information from optimization method 1 is inserted"); 
            
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

    public void ingestOpt2MetaDataToDB(ArrayList<EncOpt2MetaTableData> encOpt2MetaDataList){
        try{
            dataDbCon = dataDBConnectionPool.getConnection();
            dataDbCon.setAutoCommit(false);
            int numAllRowsAffectedInBatch = 0;

            for(EncOpt2MetaTableData eMetaData:encOpt2MetaDataList){
                PreparedStatement pst = dataDbCon.prepareStatement(inserOpt2MetaTableSQL);
                pst.setString(1, eMetaData.encD);
                pst.setString(2, eMetaData.encL);
                pst.setString(3, eMetaData.encCount);

                int numRowsAffected = pst.executeUpdate();
                numAllRowsAffectedInBatch = numAllRowsAffectedInBatch + numRowsAffected;
            }
                
            dataDbCon.commit();
            System.out.println("Rows inserted in meta information table 2 for the Optimization method 2 "); 
            
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
