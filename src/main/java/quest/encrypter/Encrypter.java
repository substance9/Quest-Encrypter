package quest.encrypter;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.util.Properties;

import quest.util.*;

public class Encrypter {
    public static void main( String[] args ){

        System.out.println( "TIPPERS Quest Program (Encrypter Module) Initializing:" );

        Properties prop = readConfig(args);

        DataIngestor dataIngestor = new DataIngestor(Integer.valueOf(prop.getProperty("db_port")),
                                                    prop.getProperty("result.enc_table_name"));

        Epoch epoch = new Epoch(Integer.parseInt(prop.getProperty("epoch")));

        DataProcessor dataProcessor = new DataProcessor(prop.getProperty("enc_key"),
                                                        prop.getProperty("secret"),
                                                        epoch,
                                                        dataIngestor);

        DataReader dataReader = new DataReader(prop.getProperty("input_path"),
                                            Integer.parseInt(prop.getProperty("max_rows")),
                                            Integer.parseInt(prop.getProperty("epoch")),
                                            epoch,
                                            dataProcessor);
        try{
            dataReader.run();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        System.out.println("Quest Batch Encryption Ends");
    }

    private static Properties readConfig( String[] args ){
        Properties prop = new Properties();

        ArgumentParser parser = ArgumentParsers.newFor("Encrypter").build().defaultHelp(true)
                .description("TIPPERS QUEST Project - Encrypter Module");
        parser.addArgument("-d", "--epoch").required(true).help("Epoch of the batch processing (in minutes)");
        parser.addArgument("-x", "--id").required(true).help("Experiment ID");
        parser.addArgument("-k", "--enc_key").required(true).help("Encryption key");
        parser.addArgument("-s", "--secret").required(true).help("Secret");
        parser.addArgument("-p", "--db_port").required(true).help("Database port");
        parser.addArgument("-n", "--enc_table_name").required(true).help("Table name for encrypted data in the database");
        parser.addArgument("-i", "--input_path").required(true).help("Data input path");
        parser.addArgument("-m", "--max_rows").required(true).help("Max rows read from data file and inserted into DB");
        parser.addArgument("-o", "--output_path").required(true).help("Result(log) output path");
		Namespace ns = null;

		try {
			ns = parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			System.exit(1);
        }
        
        if (args.length >= 2){
            //read config from command line args
            prop.setProperty("epoch", ns.get("epoch"));
            prop.setProperty("experiment_id", ns.get("id"));
            prop.setProperty("enc_key", ns.get("enc_key"));
            prop.setProperty("secret", ns.get("secret"));
            prop.setProperty("db_port", ns.get("db_port"));
            prop.setProperty("input_path", ns.get("input_path"));
            prop.setProperty("max_rows", ns.get("max_rows"));
            prop.setProperty("result.enc_table_name", ns.get("enc_table_name"));
            prop.setProperty("result.output_path", ns.get("output_path"));
        }
            
        String resultDir = prop.getProperty("result.output_path")
                                                                +"dur_"+prop.getProperty("epoch")
                                                                +"|expID_"+prop.getProperty("experiment_id");
        prop.setProperty("result.output_dir", resultDir);
        System.out.println("--result.output_dir:\t"+resultDir);
        System.out.println("--result.enc_table_name:\t"+prop.getProperty("result.enc_table_name"));

        System.out.println( "Experiment Parameters:" );
            // get the property value and print it out
            System.out.println("--epoch:\t\t\t"+prop.getProperty("epoch"));
            System.out.println("--experiment_id:\t"+prop.getProperty("experiment_id"));
            System.out.println("--enc_key:\t\t\t"+prop.getProperty("enc_key"));
            System.out.println("--secret:\t"+prop.getProperty("secret"));
            System.out.println("--db_port:\t\t\t"+prop.getProperty("db_port"));
            System.out.println("--input_path:\t"+prop.getProperty("input_path"));
            System.out.println("--max_rows:\t"+prop.getProperty("max_rows"));

        return prop;
    }
 }