package main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Calendar;
import java.util.Properties;

public class DataSetup {

	public static void main(String[] args) throws Throwable {
		
		
		Properties prop = new Properties();
		InputStream input = null;
		
		String astisdbHost = "";
		String astisdbUser = "";
		String astisdbPassword = "";
		String astisAgtdb = "";
		String astisAstisdb = "";
		String localUser = "";
		String localPassword = "";
		Feeder fd = new Feeder();
		String command= "";
		File logfile = new File("log.txt");
		Writer logout = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logfile,true), "UTF8"));
		
		try {
			 
			input = new FileInputStream("config.properties");
			prop.load(input);
			astisdbHost = prop.getProperty("astisdbHost");
			astisdbUser = prop.getProperty("astisdbUser");
			astisdbPassword = prop.getProperty("astisdbPassword");
			astisAgtdb = prop.getProperty("astisAgtdb");
			astisAstisdb = prop.getProperty("astisAstisdb");
			localUser = prop.getProperty("localUser");
			localPassword = prop.getProperty("localPassword");
		
			
		} catch (IOException ex) {
			ex.printStackTrace();
			logout.append(Calendar.getInstance().getTime() + "  " + ex + "\n");
			logout.flush();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
					logout.append(Calendar.getInstance().getTime() + "  " + e + "\n");
					logout.flush();
				}
			}
		}
		
		
		
	     

		try{
		
		command = "mysqldump -h " + astisdbHost + " --user=" + astisdbUser + " --password=" + astisdbPassword + " --databases " + astisAgtdb + " > agt.sql";
		fd.dumpData (command);
		command = "mysql " + " --user=" + localUser + " --password=" + localPassword + " agt " + " < agt.sql";
		fd.restoreData (command);
		command = "mysqldump -h " + astisdbHost + " --user=" + astisdbUser + " --password=" + astisdbPassword + " --databases " + astisAstisdb + " > astis.sql";
		fd.dumpData (command);
		command = "mysql " + " --user=" + localUser + " --password=" + localPassword + " astis " + " < astis.sql";
		fd.restoreData (command);
		
		logout.append(Calendar.getInstance().getTime() + " Geomanes Table setting start \n");
		logout.flush();
		
		GeonameSetting gnSetting = new GeonameSetting();
		gnSetting.main();
		logout.append(Calendar.getInstance().getTime() + " All the geonames Table has been set \n");
		logout.flush();
		System.out.println( "All the geonames Table has been set");
		logout.append(Calendar.getInstance().getTime() + " Start Setup the data in elasticsearch \n");
		logout.flush();
		System.out.println("Start Setup the data in elasticsearch");
		ESdataConstruction esConstruction = new ESdataConstruction();
		esConstruction.main();
		logout.append(Calendar.getInstance().getTime() + " All the data has been set in Elasticsearch \n");
		logout.flush();
		System.out.println("All the data has been set in Elasticsearch");
		dataClean dtClean = new dataClean();
		dtClean.main(null);
		
		
		}catch (Exception e){
			e.printStackTrace();
			logout.append(Calendar.getInstance().getTime() + "   " + e + "\n");
			logout.flush();
		}
		catch (Throwable e){
			e.printStackTrace();
			logout.append(Calendar.getInstance().getTime() + "   " + e + "\n");
			logout.flush();
		}finally {
			logout.flush();
			logout.close();
		}
		

	}

}
