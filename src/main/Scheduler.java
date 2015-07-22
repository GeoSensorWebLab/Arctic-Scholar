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
import java.util.Timer;
import java.util.concurrent.TimeUnit;

public class Scheduler {

	public static void main(String[] args) throws Throwable {
		
		Properties prop = new Properties();
		InputStream input = null;
		
		File logfile = new File("log.txt");
		Writer logout = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logfile,true), "UTF8"));
		
		String astisdbHost = "";
		String astisdbUser = "";
		String astisdbPassword = "";
		String astisAgtdb = "";
		String astisAstisdb = "";
		String localUser = "";
		String localPassword = "";
		String fileName = "";
		String EsSite = "";
		int crawlerPeriod = 0;
		String excepLocation = "";
		String userName = "";
		Feeder fd = new Feeder();
		String command= "";
		int firstRetriveDate = 0;
		String exceptionFileName = "";
		
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
			crawlerPeriod = Integer.parseInt(prop.getProperty("crawlerPeriod"));
			fileName = prop.getProperty("tempGenrateDataName");
			EsSite = prop.getProperty("EsSite");
			excepLocation = prop.getProperty("locationExceptionFile");
			userName = prop.getProperty("geoNameUser");
			firstRetriveDate = Integer.parseInt(prop.getProperty("firstRetriveDate"));
			exceptionFileName = prop.getProperty("exceptionFileName");
			
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		
		command = "mysqldump -h " + astisdbHost + " --user=" + astisdbUser + " --password=" + astisdbPassword + " --databases " + astisAgtdb + " > agt.sql";
		fd.dumpData (command);
		command = "mysqldump --user=" + localUser + " --password=" + localPassword + " --databases agt --tables geonames > geonames.sql";
		fd.dumpData (command);
		command = "mysqldump --user=" + localUser + " --password=" + localPassword + " --databases agt --tables location > location.sql";
		fd.dumpData (command);
		
		
		command = "mysql " + " --user=" + localUser + " --password=" + localPassword + " agt " + " < agt.sql";
		fd.restoreData (command);
		command = "mysql " + " --user=" + localUser + " --password=" + localPassword + " agt " + " < geonames.sql";
		fd.restoreData (command);
		command = "mysql " + " --user=" + localUser + " --password=" + localPassword + " agt " + " < location.sql";
		fd.restoreData (command);

		
		command = "mysqldump -h " + astisdbHost + " --user=" + astisdbUser + " --password=" + astisdbPassword + " --databases " + astisAstisdb + " > astis.sql";
		fd.dumpData (command);
		command = "mysql " + " --user=" + localUser + " --password=" + localPassword + " astis " + " < astis.sql";
		fd.restoreData (command);
		
		fd.checkAstisdb(fileName, EsSite, firstRetriveDate, exceptionFileName);
		logout.append(Calendar.getInstance().getTime() + " Astis database update sucessful" + "\n");
		logout.flush();
		fd.checkAgtdb(firstRetriveDate,  excepLocation, userName);
		logout.append(Calendar.getInstance().getTime() + " Agt database update sucessful" + "\n");
		logout.flush();
		
		
		TimeUnit.DAYS.sleep(crawlerPeriod);
		Calendar date = Calendar.getInstance();
		date.set(
		Calendar.HOUR_OF_DAY,
	    Calendar.HOUR
	    );
	    date.set(Calendar.HOUR, 0);
		date.set(Calendar.MINUTE, 0);
		date.set(Calendar.SECOND, 0);
		date.set(Calendar.MILLISECOND, 0);
		
		
		Timer time = new Timer();
		ScheduledTask schTask = new ScheduledTask();
		time.schedule(schTask,date.getTime(), crawlerPeriod*1000*60*60*24);
		
		logout.close();
	}

}
