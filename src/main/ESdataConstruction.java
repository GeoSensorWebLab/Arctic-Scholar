package main;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;


import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class ESdataConstruction {

	
	public static void main() throws SQLException, Exception, Throwable {
		astisDB astisdb = new astisDB();
		//agtDB agtdb = new agtDB();
		Properties prop = new Properties();
		InputStream input = null;
		String fileName = "";
		String EsSite = "";
		int batchNum = 0;
		String exceptionFileName = "";
		
		try {
			 
			input = new FileInputStream("config.properties");
			prop.load(input);
			batchNum = Integer.parseInt(prop.getProperty("batchNum"));
			fileName = prop.getProperty("tempGenrateDataName");
			EsSite = prop.getProperty("EsSite");
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
		
		String insertSQL = "SELECT COUNT(*) FROM astis";
		ResultSet rsetTable = astisdb.runSqlResultSet(insertSQL);
		File file = new File(exceptionFileName);
		Writer exception = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,true), "UTF8"));
		exception.close();

		Data data = new Data();
		int dataNum = 0;
		
		while(rsetTable.next()){
			dataNum = rsetTable.getInt(1);
			System.out.println(dataNum);
		}
		for (int i=1; i <= ((dataNum/batchNum)+1); i++){
			insertSQL = "SELECT * FROM astis WHERE SISN > " + (i*batchNum-batchNum) + " and SISN <" + (i*batchNum+1);
			System.out.println(insertSQL);
			data.Generation(fileName,insertSQL);
			data.dataBatch (EsSite,fileName,exceptionFileName);	
		}
		
		data.fileDeletion(fileName);

	}
	

}
