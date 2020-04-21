package quest.encrypter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import quest.model.EncWifiData;
import quest.model.RawWifiData;
import quest.util.AES;
import quest.util.Epoch;

public class DataProcessor {
    private String secretZStr;
    private String keyStr;
    private ArrayList<RawWifiData> currBatchRawDataList;
    private ArrayList<EncWifiData> currBatchEncDataList;
    private int anyLocMaxCount;
    private Random randGenerator;
    private String fakeStringForEncCL;
    private DataIngestor dataIngestor;
    private Epoch epoch;

    public DataProcessor(String keyStr, String secretZStr, Epoch epoch, DataIngestor dataIngestor) {
        this.keyStr = keyStr;
        this.secretZStr = secretZStr;
        currBatchRawDataList = new ArrayList<RawWifiData>();
        anyLocMaxCount = 0;
        randGenerator = new Random();
        fakeStringForEncCL = "dskfjlkajsdiofjioajsidofjolskdfjlk";
        this.dataIngestor = dataIngestor;
        this.epoch = epoch;
    }

    public void addToCurrBatch(RawWifiData rData) {
        currBatchRawDataList.add(rData);
    }

    public void processCurrBatch() {
        currBatchEncDataList = encryptBatch(currBatchRawDataList);

        dataIngestor.ingestBatchToDB(currBatchEncDataList);

        currBatchRawDataList = new ArrayList<RawWifiData>();
        currBatchEncDataList = new ArrayList<EncWifiData>();
    }

    public ArrayList<EncWifiData> encryptBatch(ArrayList<RawWifiData> rDataBatch) {
        //debug
        RawWifiData firstData = rDataBatch.get(0);
        String batchStartTimeStr = firstData.time.toString();
        String batchIdStr = epoch.getEpochIdStrByMs(firstData.time.getTime());
        System.out.println("Processing Batch: " + batchIdStr);
        System.out.println("--First data time: " + batchStartTimeStr);
        //...........

        ArrayList<EncWifiData> eDataBatch = new ArrayList<EncWifiData>();

        HashMap<String, HashSet<String>> devVisitedLocSetMap = new HashMap<String, HashSet<String>>();
        HashSet<String> hashSetId = new HashSet<String>();
        HashSet<String> hashSetLoc = new HashSet<String>();

        HashMap<String, Integer> locVisitedCounterMap = new HashMap<String, Integer>();

        HashMap<String, String> devAllVisitedLocStrMap = new HashMap<String, String>();

        String encId = null;
        String encU = null;
        String encL = null;
        String encCL = null;
        String encD = null;

        int rowCounter = 0;
        long firstDataTimeMs = rDataBatch.get(0).time.getTime();
        long epochId = epoch.getEpochIdByMs(firstDataTimeMs);
        String epochIdStr = String.valueOf(epochId);

        // Line 2
        for (RawWifiData rData : currBatchRawDataList) {
            if (!devVisitedLocSetMap.containsKey(rData.clientId)) {
                HashSet<String> devVisitedLocSet = new HashSet<String>();
                devVisitedLocSet.add(rData.apId);
                devVisitedLocSetMap.put(rData.clientId, devVisitedLocSet);
            } else {
                HashSet<String> devVisitedLocSet = devVisitedLocSetMap.get(rData.clientId);
                if (!devVisitedLocSet.contains(rData.apId)) {
                    devVisitedLocSet.add(rData.apId);
                    devVisitedLocSetMap.replace(rData.clientId, devVisitedLocSet);
                } else {
                    continue;
                }
            }
        }

        for (String clientId : devVisitedLocSetMap.keySet()) {
            HashSet<String> locSet = devVisitedLocSetMap.get(clientId);
            int n = locSet.size(); 
            String locArray[] = new String[n]; 
            locArray = locSet.toArray(locArray);
            devAllVisitedLocStrMap.put(clientId, String.join(",", locArray));
        }

        devVisitedLocSetMap = new HashMap<String, HashSet<String>>();

        //Line 4
        for (RawWifiData rData:currBatchRawDataList){
            rowCounter = rowCounter + 1;
            int r = randGenerator.nextInt(Integer.MAX_VALUE); // what should be the max of rand int?
            String rStr = String.valueOf(r);
            //Line 6-8, Encrypting device-ids (A_id) and Uniqueness of the device (A_u)
            //Line 6
            if (!hashSetId.contains(rData.clientId)){
                // shouldn't add, because later will be used again for deciding hashSetId.add(rData.clientId);
                encId = AES.concatAndEncrypt(rData.clientId,"1",secretZStr);
                encU = AES.concatAndEncrypt("1",String.valueOf(rowCounter),epochIdStr);
                HashSet<String> devVisitedLocSet = new HashSet<String>(); 
                devVisitedLocSet.add(rData.apId);
                devVisitedLocSetMap.put(rData.clientId, devVisitedLocSet);
            }
            else{
                HashSet<String> devVisitedLocSet = devVisitedLocSetMap.get(rData.clientId);
                //Line 7
                if (!devVisitedLocSet.contains(rData.apId)){
                    encId = AES.concatAndEncrypt(rData.clientId,rStr,secretZStr);
                    encU = AES.concatAndEncrypt("1",String.valueOf(rowCounter),epochIdStr);
                    devVisitedLocSet.add(rData.apId);
                    devVisitedLocSetMap.replace(rData.clientId, devVisitedLocSet);
                }
                //Line 8
                else{
                    encId = AES.concatAndEncrypt(rData.clientId,rStr,secretZStr);
                    encU = AES.concatAndEncrypt("0",rStr);
                }
            }

            //Line 9-12
            if(!hashSetLoc.contains(rData.apId)){
                hashSetLoc.add(rData.apId);
                int locVisitedCounter = 1;
                locVisitedCounterMap.put(rData.apId, locVisitedCounter);
                encL = AES.concatAndEncrypt(rData.apId, String.valueOf(locVisitedCounter));
                //Line 9
                if(!hashSetId.contains(rData.clientId)){
                    hashSetId.add(rData.clientId);
                    String allVisitedLocStr = devAllVisitedLocStrMap.get(rData.clientId);
                    encCL = AES.concatAndEncrypt(rStr,allVisitedLocStr);
                }
                //Line 10
                else{
                    encCL = AES.concatAndEncrypt(fakeStringForEncCL,String.valueOf(rowCounter));
                }
            }
            //Line 11-12
            else{
                int locVisitedCounter = locVisitedCounterMap.get(rData.apId);
                locVisitedCounter = locVisitedCounter + 1;
                locVisitedCounterMap.replace(rData.apId, locVisitedCounter);
                encL = AES.concatAndEncrypt(rData.apId, String.valueOf(locVisitedCounter));
                //Line 11
                if(!hashSetId.contains(rData.clientId)){
                    hashSetId.add(rData.clientId);
                    String allVisitedLocStr = devAllVisitedLocStrMap.get(rData.clientId);
                    encCL = AES.concatAndEncrypt(rStr,allVisitedLocStr);
                }
                //Line 12
                else{
                    encCL = AES.concatAndEncrypt(fakeStringForEncCL,String.valueOf(rowCounter));
                }
            }

            encD = AES.encrypt(epochIdStr);

            //TODO: MAX Value

            EncWifiData eData = new EncWifiData(encId, encU, encL, encCL, encD);
            eDataBatch.add(eData);
        }

        return eDataBatch;
    }
}
