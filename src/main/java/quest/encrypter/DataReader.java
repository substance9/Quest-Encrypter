package quest.encrypter;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import quest.model.RawWifiData;

public class DataReader {
    private File inputFilesPath;
    private long durationInMs;
    private DataProcessor dataProcessor;

    public DataReader(String inputFilesPathStr, int duration, DataProcessor processor) {
        inputFilesPath = new File(inputFilesPathStr);
        durationInMs = duration * 60 * 1000;
        dataProcessor = processor;
    }

    public void run() throws IOException {
        //Get the list of files in the input directory and sort the files according to the file names
        List<File> inputFiles = Arrays.asList(inputFilesPath.listFiles());
        Collections.sort(inputFiles);

        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        for (File inputFile : inputFiles) {
            //Skip the directories, e.g. ..,. or other sub dirs
            if (inputFile.isDirectory()) {
                continue;
            } else {
                //Only read CSV data files
                if (!inputFile.getName().split("\\.", 2)[1].equals("csv")) {
                    continue;
                }
                System.out.println("Processing File: " + inputFile.getName());

                try (Reader reader = new FileReader(inputFile);
                    CSVParser csvParser = new CSVParser(reader,
                    CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());)
                {
                    //For each CSV data file, maintain following variables to track timestamp
                    long durationStartEpoch = 0;
                    long currentDataEpoch = 0;
                    Timestamp currentDataTs = null;

                    for (CSVRecord csvRecord : csvParser) {
                        String timeStr = csvRecord.get("time");
                        String apIdStr = csvRecord.get("ap_id");
                        String clientIdStr = csvRecord.get("client_id");

                        try {
                            Date date = sdf.parse(timeStr);
                            currentDataEpoch = date.getTime();
                            currentDataTs = new Timestamp(currentDataEpoch);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return;
                        }

                        RawWifiData rData = new RawWifiData(currentDataTs, apIdStr, clientIdStr);

                        if (durationStartEpoch == 0){
                            durationStartEpoch = currentDataEpoch;
                        }

                        if (currentDataEpoch - durationStartEpoch > durationInMs){
                            dataProcessor.processCurrBatch();
                            durationStartEpoch = currentDataEpoch;
                        }
  
                        dataProcessor.addToCurrBatch(rData);
                    }
                }
            }
            System.out.println("Processing File: " + inputFile.getName() + " Done");
            // debug
        }
    }
}
