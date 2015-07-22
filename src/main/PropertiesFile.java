package main;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public class PropertiesFile {

	public static void main(String[] args) {
		Properties prop = new Properties();
		OutputStream output = null;

		try {

			output = new FileOutputStream("config.properties");

			// set the properties value
			prop.setProperty("JDBCdriver","com.mysql.jdbc.Driver");
			prop.setProperty("dbagtUrl", "jdbc:mysql://localhost:3306/agt");
			prop.setProperty("localUser", "");
			prop.setProperty("localPassword", "");
			prop.setProperty("dbastisUrl", "jdbc:mysql://localhost:3306/astis");
			prop.setProperty("geoNameUser", "");
			prop.setProperty("exceptionFileName", "exception.json");
			prop.setProperty("tempGenrateDataName", "generateData.json");
			prop.setProperty("EsSite", "localhost:9200/arctic/data/_bulk?pretty");
			prop.setProperty("batchNum","20");
			prop.setProperty("locationExceptionFile","excepLocation.txt");
			prop.setProperty("GeonameNum","2000");
			prop.setProperty("astisdbHost","");
			prop.setProperty("astisdbUser","");
			prop.setProperty("astisdbPassword","");
			prop.setProperty("astisAgtdb","agt");
			prop.setProperty("astisAstisdb","astis");
			prop.setProperty("crawlerPeriod","1");
			prop.setProperty("firstRetriveDate","10");

			// save properties to project root folder
			prop.store(output, null);

		} catch (IOException io) {
			io.printStackTrace();
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}

	}

}
