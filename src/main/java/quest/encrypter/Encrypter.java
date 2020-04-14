package quest.encrypter;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.util.Properties;

public class Encrypter {
    public static void main( String[] args ){

        System.out.println( "TIPPERS Quest Program (Encrypter Module) Initializing:" );

        Properties prop = readConfig(args);

        DataIngestor dataIngestor = new DataIngestor(Integer.valueOf(prop.getProperty("db_port")),
                                                    prop.getProperty("result.enc_table_name"));

        DataProcessor dataProcessor = new DataProcessor(prop.getProperty("enc_key"),
                                                        prop.getProperty("secret"),
                                                        dataIngestor);

        DataReader dataReader = new DataReader(prop.getProperty("input_path"),
                                            Integer.parseInt(prop.getProperty("duration")),
                                            dataProcessor);
        try{
            dataReader.run();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static Properties readConfig( String[] args ){
        Properties prop = new Properties();

        ArgumentParser parser = ArgumentParsers.newFor("Encrypter").build().defaultHelp(true)
                .description("TIPPERS QUEST Project - Encrypter Module");
        parser.addArgument("-d", "--duration").required(true).help("duration of the batch processing (in minutes)");
        parser.addArgument("-x", "--id").required(true).help("Experiment ID");
        parser.addArgument("-k", "--enc_key").required(true).help("Encryption key");
        parser.addArgument("-s", "--secret").required(true).help("Secret");
        parser.addArgument("-p", "--db_port").required(true).help("Database port");
        parser.addArgument("-n", "--enc_table_name").required(true).help("Table name for encrypted data in the database");
        parser.addArgument("-i", "--input_path").required(true).help("Data input path");
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
            prop.setProperty("duration", ns.get("duration"));
            prop.setProperty("experiment_id", ns.get("id"));
            prop.setProperty("enc_key", ns.get("enc_key"));
            prop.setProperty("secret", ns.get("secret"));
            prop.setProperty("db_port", ns.get("db_port"));
            prop.setProperty("input_path", ns.get("input_path"));
            prop.setProperty("result.enc_table_name", ns.get("enc_table_name"));
            prop.setProperty("result.output_path", ns.get("output_path"));
        }
            
        String resultDir = prop.getProperty("result.output_path")
                                                                +"dur_"+prop.getProperty("duration")
                                                                +"|expID_"+prop.getProperty("experiment_id");
        prop.setProperty("result.output_dir", resultDir);
        System.out.println("--result.output_dir:\t"+resultDir);
        System.out.println("--result.enc_table_name:\t"+prop.getProperty("result.enc_table_name"));

        System.out.println( "Experiment Parameters:" );
            // get the property value and print it out
            System.out.println("--duration:\t\t\t"+prop.getProperty("duration"));
            System.out.println("--experiment_id:\t"+prop.getProperty("experiment_id"));
            System.out.println("--enc_key:\t\t\t"+prop.getProperty("enc_key"));
            System.out.println("--secret:\t"+prop.getProperty("secret"));
            System.out.println("--db_port:\t\t\t"+prop.getProperty("db_port"));
            System.out.println("--input_path:\t"+prop.getProperty("input_path"));

        return prop;
    }
 }