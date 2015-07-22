package main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Feeder {

	public void feeder() throws Throwable {
		Properties prop = new Properties();
		InputStream input = null;
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
		String exceptionFileName = "";
		
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
			crawlerPeriod = Integer.parseInt(prop.getProperty("crawlerPeriod"));
			fileName = prop.getProperty("tempGenrateDataName");
			EsSite = prop.getProperty("EsSite");
			excepLocation = prop.getProperty("locationExceptionFile");
			userName = prop.getProperty("geoNameUser");
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
		
		String command = "mysqldump -h " + astisdbHost + " --user=" + astisdbUser + " --password=" + astisdbPassword + " --databases " + astisAgtdb + " > agt.sql";
		dumpData (command);
		command = "mysqldump --user=" + localUser + " --password=" + localPassword + " --databases agt --tables geonames > geonames.sql";
		//System.out.println(command);
		dumpData (command);
		command = "mysqldump --user=" + localUser + " --password=" + localPassword + " --databases agt --tables location > location.sql";
		//System.out.println(command);
		dumpData (command);
		
		
		command = "mysql " + " --user=" + localUser + " --password=" + localPassword + " agt " + " < agt.sql";
		restoreData (command);
		command = "mysql " + " --user=" + localUser + " --password=" + localPassword + " agt " + " < geonames.sql";
		restoreData (command);
		command = "mysql " + " --user=" + localUser + " --password=" + localPassword + " agt " + " < location.sql";
		restoreData (command);

		
		command = "mysqldump -h " + astisdbHost + " --user=" + astisdbUser + " --password=" + astisdbPassword + " --databases " + astisAstisdb + " > astis.sql";
		dumpData (command);
		command = "mysql " + " --user=" + localUser + " --password=" + localPassword + " astis " + " < astis.sql";
		restoreData (command);
		
		checkAstisdb(fileName, EsSite, crawlerPeriod, exceptionFileName);
		logout.append(Calendar.getInstance().getTime() + " Astis database update sucessful" + "\n");
		logout.flush();
		checkAgtdb(crawlerPeriod,  excepLocation, userName);
		logout.append(Calendar.getInstance().getTime() + " Agt database update sucessful"+ "\n");
		logout.flush();
		logout.close();

		
	}
	
	public void dumpData(String command) throws IOException, InterruptedException {
		
		StringBuffer output = new StringBuffer(); 
		File logfile = new File("log.txt");
		Writer logout = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logfile,true), "UTF8"));
		
		 try {
	            Runtime runtime = Runtime.getRuntime();
	            Process process = runtime.exec(new String[] { "/bin/bash", "-c", command });
	            int exitValue = process.waitFor();
	            //System.out.println("exit value: " + exitValue);
	            BufferedReader buf = new BufferedReader(new InputStreamReader(process.getInputStream()));
	            String line = "";
	            while ((line = buf.readLine()) != null) {
	            	output.append(line + "\n");
	            }
	            if (exitValue==0){
	            	logout.append(Calendar.getInstance().getTime() + " .sql file dump success"+ "\n");
	            	System.out.println(Calendar.getInstance().getTime() + " .sql file dump success"+ "\n");
	            }else{
	            	System.out.println(Calendar.getInstance().getTime() + " .sql file dump failure" + "\n");
	            	logout.append(Calendar.getInstance().getTime() + " .sql file dump failure"+ "\n");
	            }
	            
	            logout.close();
	            
	        } catch (Exception e) {
	            System.out.println(e);
	        }
		
	}
	
	public void restoreData (String command) throws IOException {
		
		File logfile = new File("log.txt");
		Writer logout = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logfile,true), "UTF8"));
		
		try {
			Process runtimeProcess =Runtime.getRuntime().exec(new String[] { "/bin/bash", "-c", command });
			int processComplete = runtimeProcess.waitFor();
			if(processComplete == 0){
				System.out.println(Calendar.getInstance().getTime()  + " .sql restore success"+ "\n");
				logout.append(Calendar.getInstance().getTime()  + " .sql restore success"+ "\n");
			} else {
				System.out.println(Calendar.getInstance().getTime() + " .sql restore failure"+ "\n");
				logout.append(Calendar.getInstance().getTime() + " .sql restore failure"+ "\n");
			}
			
			logout.close();
		} catch (Exception e) {
            System.out.println(e);
        }
	}

	public void checkAstisdb (String fileName, String EsSite, int crawlerPeriod, String exceptionFileName) throws Throwable {
		
		File logfile = new File("log.txt");
		Writer logout = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logfile,true), "UTF8"));
		
		try{
		astisDB astisdb = new astisDB();
		ResultSet rsetTable;
		String insertSQL = "SELECT SISN,DM FROM astis";
		rsetTable = astisdb.runSqlResultSet(insertSQL);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date currentDate = new Date();
		Data data = new Data();
		int diffDay = 0;
		
		int UpdateNum = 0;
		
		
		
		while (rsetTable.next()){
			try {
				if(String.valueOf(rsetTable.getObject(2))!="null"){
					diffDay = (int) TimeUnit.DAYS.convert(currentDate.getTime() - dateFormat.parse(rsetTable.getObject(2).toString()).getTime(), TimeUnit.MILLISECONDS);
					if(diffDay <= (crawlerPeriod)){
						UpdateNum++;
					}
				}
			}catch (ParseException e) {
			    e.printStackTrace();
			    logout.append(Calendar.getInstance().getTime() + "  " + e + "\n");
			    logout.flush();
			}catch (Throwable e){
				e.printStackTrace();
				logout.append(Calendar.getInstance().getTime() + "  " + e + "\n");
				logout.flush();
			}
		}
		System.out.println(UpdateNum);
		
	    int[]dateRecord = new int [UpdateNum];
		UpdateNum = 0;
		astisdb.connect();
		rsetTable = astisdb.runSqlResultSet(insertSQL);
		
		while (rsetTable.next()){
			try {
				if(String.valueOf(rsetTable.getObject(2))!="null"){
					diffDay = (int) TimeUnit.DAYS.convert(currentDate.getTime() - dateFormat.parse(rsetTable.getObject(2).toString()).getTime(), TimeUnit.MILLISECONDS);
					if(diffDay <= (crawlerPeriod)){
						dateRecord[UpdateNum] = rsetTable.getInt(1);
						UpdateNum ++;
					}
				}
			}catch (ParseException e) {
				logout.append(Calendar.getInstance().getTime() + "  " + e + "\n");
			    e.printStackTrace();
			    logout.flush();
			}catch (Throwable e){
				e.printStackTrace();
				logout.append(Calendar.getInstance().getTime() + "  " + e + "\n");
				logout.flush();
			}
		}
		
		for (int i =0; i<UpdateNum; i++){
			insertSQL = "SELECT * FROM astis where SISN = " + dateRecord[i];
			try {
				logout.append(Calendar.getInstance().getTime()  + " Updated ASTIS Data SISN = " + dateRecord[i] +"\n");
				logout.flush();
				data.Generation(fileName, insertSQL);
				data.dataBatch(EsSite, fileName,exceptionFileName);
			} catch (IOException e) {
				e.printStackTrace();
				logout.append(Calendar.getInstance().getTime() + "  " + e + "\n");
				logout.flush();
			} catch (Throwable e) {
				e.printStackTrace();
				logout.append(Calendar.getInstance().getTime() + "  " + e + "\n");
				logout.flush();
			}
		}
		} catch (SQLException e) {
			e.printStackTrace();
			logout.append(Calendar.getInstance().getTime() + "  " + e + "\n");
			logout.flush();
		} catch (Throwable e) {
			e.printStackTrace();
			logout.append(Calendar.getInstance().getTime() + "  " + e + "\n");
			logout.flush();
		}
		
		logout.flush();
		logout.close();
		
	}
	
	public void checkAgtdb (int crawlerPeriod, String excepLocation, String userName) throws Throwable {
		
		File logfile = new File("log.txt");
		Writer logout = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logfile,true), "UTF8"));
		try {
		agtDB agtdb = new agtDB();
		ResultSet rsetTable;
		String insertSQL = "SELECT SISN,DATE_MODIFIED FROM agt";
		rsetTable = agtdb.runSqlResultSet(insertSQL);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date currentDate = new Date();
		boolean rsetValue = false;
		ArrayList<String> mdData = new ArrayList<String>();
		
		int diffDay = 0;
		
		while (rsetTable.next()){
			if(String.valueOf(rsetTable.getObject(2))!="null"){
				diffDay = (int) TimeUnit.DAYS.convert(currentDate.getTime() - dateFormat.parse(rsetTable.getObject(2).toString()).getTime(), TimeUnit.MILLISECONDS);
				if(diffDay <= (crawlerPeriod)){
					mdData.add(rsetTable.getObject(1).toString());
				}
			}
		}
		
		if (mdData.isEmpty() == false){
			GeonameSetting gnSet = new GeonameSetting();
			gnSet.tableSetting();
			agtdb.runSql2("TRUNCATE location;");
			gnSet.locationTableSetting (excepLocation);
			gnSet.engColumParse ();
			
			for (int i=0;i<mdData.size();i++){
				
				insertSQL = "SELECT * FROM geonames WHERE id = " + mdData.get(i);
				rsetTable = agtdb.runSqlResultSet(insertSQL);
				rsetValue = rsetTable.next();
				if (rsetValue == false){
					insertSQL = "SELECT * FROM agt WHERE SISN = " + mdData.get(i);
					setGeonameTable (userName, insertSQL);
					logout.append(Calendar.getInstance().getTime()  + " Updated AGT Data SISN = " + mdData.get(i) +"\n");
					logout.flush();
				}else{
					insertSQL = "SELECT * FROM agt WHERE SISN = " + mdData.get(i);
					updateGeonameTable (userName, insertSQL);
					logout.append(Calendar.getInstance().getTime()  + " Updated AGT Data SISN = " + mdData.get(i) +"\n");
					logout.flush();
				}
				
			}
			
			
			
		}
		} catch (SQLException e) {
			e.printStackTrace();
			logout.append(Calendar.getInstance().getTime() + "  " + e + "\n");
			logout.flush();
		} catch (Throwable e) {
			e.printStackTrace();
			logout.append(Calendar.getInstance().getTime() + "  " + e + "\n");
			logout.flush();
		}
		logout.close();
		
	}


	public void setGeonameTable (String userName, String insertSQL) throws SQLException, IOException  {
		
		agtDB agtdb = new agtDB();
		JsonObject firstObj = new JsonObject();
		float lat;
		float lng;
		String geoUrl;
		ResultSet rsetAgt= agtdb.runSqlResultSet(insertSQL);
	    ResultSet rsetScope;	
	    String output;
	    int resultCount; 
	    JsonObject geoObject=new JsonObject();
	    JsonArray geonames=new JsonArray();
	    String engDesc;
	    
	    
	    
	    while(rsetAgt.next()){
	    	//System.out.println(rsetAgt.getInt("SISN"));
	    	engDesc = rsetAgt.getString("ENG_DESC");
	    	if (engDesc== null){
	    		insertSQL= "INSERT INTO  geonames"+"(id,geoYN)"+"VALUES"+
						"("+ rsetAgt.getInt("SISN") +","+"\""+"O"+"\""+")";
				agtdb.runSql(insertSQL);
	    	}else{
	    	
	    		insertSQL="SELECT * FROM location WHERE SISN = " + rsetAgt.getInt("SISN");
	    		//System.out.println(insertSQL);
	    		//System.out.println(rsetAgt.getString("ENG_DESC"));
	    		rsetScope = agtdb.runSqlResultSet(insertSQL);
	    		geoUrl = "http://api.geonames.org/searchJSON?q=" + URLEncoder.encode(rsetAgt.getString("ENG_DESC").replace("region",""), "UTF-8") + "&username="+userName;
	    	
	    		if (rsetScope.next()){
	    			insertSQL= "INSERT INTO  geonames"+"(id,name,lat,lng,geoYN)"+"VALUES"+
	    					"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC")+"\""+","+rsetScope.getFloat("lat")+","+rsetScope.getFloat("lng")+","+"\""+"N"+"\""+")";
	    			agtdb.runSql(insertSQL);
	    			
	    		}else{
    				geoUrl = "http://api.geonames.org/searchJSON?q=" + URLEncoder.encode(rsetAgt.getString("ENG_DESC").replace("region",""), "UTF-8") + "&username="+userName;
    				output = connectToPage(geoUrl);
    				geoObject=(JsonObject)new JsonParser().parse(output);
    				resultCount = geoObject.get("totalResultsCount").getAsInt();
    				geonames = geoObject.get("geonames").getAsJsonArray();
    				if (resultCount == 0){
    					insertSQL= "INSERT INTO  geonames"+"(id,name,geoYN)"+"VALUES"+
    							"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC")+"\""+","+"\""+"O"+"\""+")";
    					agtdb.runSql(insertSQL);
    				}else {
    					firstObj=geonames.get(0).getAsJsonObject();
    					lat=firstObj.get("lat").getAsFloat();
    					lng=firstObj.get("lng").getAsFloat();
    					insertSQL= "INSERT INTO  geonames"+"(id,name,lat,lng,geoYN)"+"VALUES"+
    								"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC")+"\""+","+lat+","+lng+","+"\""+"Y"+"\""+")";
    					agtdb.runSql(insertSQL);
    					
    				}
    			}
	    	}
	    		 	
	    }
	    
	    System.out.println("geonames done");
	}
	
	public void updateGeonameTable (String userName, String insertSQL) throws Throwable  {
		
		agtDB agtdb = new agtDB();
		JsonObject firstObj = new JsonObject();
		float lat;
		float lng;
		String geoUrl;
		ResultSet rsetAgt= agtdb.runSqlResultSet(insertSQL);
	    ResultSet rsetScope;	
	    String output;
	    int resultCount; 
	    JsonObject geoObject=new JsonObject();
	    JsonArray geonames=new JsonArray();
	    String engDesc;
	    
	    
	    
	    while(rsetAgt.next()){
	    	//System.out.println(rsetAgt.getInt("SISN"));
	    	engDesc = rsetAgt.getString("ENG_DESC");
	    	if (engDesc== null){
	    		insertSQL= "UPDATE geonames SET " + "geoYN = " +"\""+"O"+"\" " + "Where id = " + rsetAgt.getInt("SISN");
				agtdb.runSql(insertSQL);
	    	}else{
	    	
	    		insertSQL="SELECT * FROM location WHERE SISN = " + rsetAgt.getInt("SISN");
	    		rsetScope = agtdb.runSqlResultSet(insertSQL);
	    		geoUrl = "http://api.geonames.org/searchJSON?q=" + URLEncoder.encode(rsetAgt.getString("ENG_DESC").replace("region",""), "UTF-8") + "&username="+userName;
	    	
	    		if (rsetScope.next()){
	    			insertSQL= "UPDATE geonames SET " + "name = "  +"\""+ rsetAgt.getString("ENG_DESC")  +"\""+ ", lat = " + rsetScope.getFloat("lat")+ ",lng = " +
	    						rsetScope.getFloat("lng") + ",geoYN = \"N\" WHERE id = " + rsetAgt.getInt("SISN");
	    			agtdb.runSql(insertSQL);
	    			
	    		}else{
    				geoUrl = "http://api.geonames.org/searchJSON?q=" + URLEncoder.encode(rsetAgt.getString("ENG_DESC").replace("region",""), "UTF-8") + "&username="+userName;
    				output = connectToPage(geoUrl);
    				geoObject=(JsonObject)new JsonParser().parse(output);
    				resultCount = geoObject.get("totalResultsCount").getAsInt();
    				geonames = geoObject.get("geonames").getAsJsonArray();
    				if (resultCount == 0){
    					insertSQL= "UPDATE geonames SET "+ "name = "  +"\""+ rsetAgt.getString("ENG_DESC")  +"\""+ ",geoYN = \"O\" WHERE id = " + rsetAgt.getInt("SISN");
    					agtdb.runSql(insertSQL);
    				}else {
    					firstObj=geonames.get(0).getAsJsonObject();
    					lat=firstObj.get("lat").getAsFloat();
    					lng=firstObj.get("lng").getAsFloat();
    					insertSQL= "UPDATE geonames SET "+ "name = "  +"\"" + rsetAgt.getString("ENG_DESC")  +"\""+ ", lat = " + lat + ",lng = " + lng +
    								",geoYN = \"Y\" WHERE id = " + rsetAgt.getInt("SISN");
    					agtdb.runSql(insertSQL);
    					
    				}
    			}
	    	}
	    		 	
	    }
	    
	    agtdb.finalize();
	    System.out.println("geonames done");
	}
	
	
	public static String connectToPage(String pageURL){
		try {
			 
			URL url = new URL(pageURL);
			//System.out.println(url);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
	 
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ conn.getResponseCode());
			}
	 
			BufferedReader br = new BufferedReader(new InputStreamReader(
				(conn.getInputStream())));
	 
			String output;
			output = br.readLine();
			//System.out.println("Output from Server .... ");
			//System.out.println(output);
			conn.disconnect();
			return output;
			
		} catch (MalformedURLException e) {
			 
			e.printStackTrace();
			return ("MalformedURLEXception e");
	 
		  } catch (IOException e) {
	 
			e.printStackTrace();
			return ("IOException e");
	 
		  }	
	}	
	
	
	
	
}
