package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.URLEncoder;

import com.google.gson.JsonParser;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;

public class GeonameSetting {

	
	
	public void main() throws Throwable  {
		
		agtDB agtdb = new agtDB();
		
		Properties prop = new Properties();
		InputStream input = null;
		int geonameNum = 0;
		String excepLocation = "";
		String insertSQL = "";
		int agtNum = 0;
		String userName = "";
		
		try {
			 
			input = new FileInputStream("config.properties");
			prop.load(input);
			geonameNum = Integer.parseInt(prop.getProperty("GeonameNum"));
			excepLocation = prop.getProperty("locationExceptionFile");
			userName = prop.getProperty("geoNameUser");
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
		
		
		tableSetting();
		agtdb.runSql2("TRUNCATE location;");
		agtdb.runSql2("TRUNCATE geonames;");
		locationTableSetting (excepLocation);
		engColumParse ();
		
		
		insertSQL = "SELECT COUNT(*) FROM agt";
		ResultSet rsetAgt= agtdb.runSqlResultSet(insertSQL);
		rsetAgt.next();
		agtNum = rsetAgt.getInt(1);
		int i =1,k=0, startNum = 1, endNum = 1;
		insertSQL = "SELECT SISN FROM location";
		ResultSet rsetLocation = agtdb.runSqlResultSet(insertSQL);
		rsetLocation.last();
		int[] locationSISN = new int[rsetLocation.getRow()];
		rsetLocation.first();
		for (i=0;i<locationSISN.length;i++){
			locationSISN[i] = rsetLocation.getInt(1);
			rsetLocation.next();
		}
		i = 1;
		while(i<=agtNum){
			int j = 1;
			startNum = i;
			while (j<=geonameNum){
				if (locationSISN[k]==i){
					i++;k++;
				}else {
					j++; i++;
				}
			}
			endNum = i;
			insertSQL = "SELECT * FROM agt WHERE SISN > " + startNum +" AND SISN < " + (endNum+1) ;
			try {
				agtdb = new agtDB();
				setGeonameTable(userName,insertSQL);
				agtdb.finalize();
				if ((endNum+1)<agtNum){
					TimeUnit.DAYS.sleep(1);
				}
			} catch(InterruptedException ex) {
			    Thread.currentThread().interrupt();
			}
		}
		
	}
	
	public void tableSetting () throws Throwable {
		
		//Location table create
		agtDB agtdb = new agtDB();
		
		String createTableSQL = "CREATE TABLE IF NOT EXISTS location ("
						+ "SISN INT(11) NOT NULL, "
						+ "name VARCHAR(100), "
						+ "scope VARCHAR(2000), "
						+ "coor VARCHAR(100),"
						+ "lat DECIMAL(12,6),"
						+ "lng DECIMAL(12,6)"
						+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";
				
			agtdb.runSql2(createTableSQL);
			
				//Geoname table create
				createTableSQL = "CREATE TABLE IF NOT EXISTS geonames ("
						+ "id INT(11) NOT NULL,"
						+ "name VARCHAR(100),"
						+ "lat DECIMAL(12,6), "
						+ "lng DECIMAL(12,6),"
						+ "geoYN VARCHAR(2)"
						+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";
						
				agtdb.runSql2(createTableSQL);		
		agtdb.finalize();
				
	}
	
	public void locationTableSetting (String excepLocation) throws Throwable {
		
		agtDB agtdb = new agtDB();
		String insertSQL;
		insertSQL = "SELECT * FROM agt";
		ResultSet rsetAgt= agtdb.runSqlResultSet(insertSQL);
	    ResultSet rsetScope;	
	    int rowNum; 
	    String scopeDes;
	    String desWords[],latWords[],lngWords[];
	    String engDesc;
	    StringBuilder content = new StringBuilder();
	    String contentResult="";
	    Pattern pattern = Pattern.compile("\\d+\\s\\d+\\s(\\d+\\s)?[NS],\\s\\d+\\s\\d+\\s(\\d+\\s)?[WE].");   
	    Matcher matcher = pattern.matcher("");
	    Pattern patlng = Pattern.compile("\\d+\\s\\d+\\s(\\d+\\s)?[WE]");
	    Pattern patlat = Pattern.compile("\\d+\\s\\d+\\s(\\d+\\s)?[NS]");
	    Matcher matlng = patlng.matcher("");
	    Matcher matlat = patlat.matcher("");
	    String coor,longitude,latitude;
	    Float lng,lat;
		
	    File file = new File(excepLocation);
	    Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF8"));
	    
	    
	    while(rsetAgt.next()){
	    	engDesc = rsetAgt.getString("ENG_DESC");
	    	if (engDesc== null){
	    	}else{
	    	
	    	insertSQL="SELECT * FROM agt_eng_scope WHERE SISN = " + rsetAgt.getInt("SISN");
	    	//System.out.println(insertSQL);
	    	//System.out.println(rsetAgt.getString("ENG_DESC"));
	    	rsetScope = agtdb.runSqlResultSet(insertSQL);
	    	rowNum=0;
	    	if (rsetScope.next()){
	    		rsetScope.last();
	    		rowNum=rsetScope.getRow();
	    		rsetScope.first();
	    		if (rowNum>1){
	    			do{
	    				scopeDes=rsetScope.getString("ENG_SCOPE");
	    				desWords=scopeDes.trim().split(" ");
	    				matcher = pattern.matcher(scopeDes);
	    				if (desWords[0].contains("Location")){
	    					if (matcher.find()){
	    						coor = matcher.group();
	    						matlng = patlng.matcher(coor);
	    						matlng.find();
	    						longitude = matlng.group();
	    						matlat = patlat.matcher(coor);
	    						matlat.find();
	    						latitude = matlat.group();
	    						latWords = latitude.trim().split(" ");
	    						lngWords = longitude.trim().split(" ");
	    						scopeDes=scopeDes.replace("\"", "\\\"");
	    						if (latWords.length==4){
	    							lat = latWords[3].contains("S")? -1*((((Float.parseFloat(latWords[2])/60)+Float.parseFloat(latWords[1]))/60)+Float.parseFloat(latWords[0])) : ((((Float.parseFloat(latWords[2])/60)+Float.parseFloat(latWords[1]))/60)+Float.parseFloat(latWords[0]));
	    							if (lngWords.length==4){
		    							lng = lngWords[3].contains("W")? -1*((((Float.parseFloat(lngWords[2])/60)+Float.parseFloat(lngWords[1]))/60)+Float.parseFloat(lngWords[0])) : ((((Float.parseFloat(lngWords[2])/60)+Float.parseFloat(lngWords[1]))/60)+Float.parseFloat(lngWords[0]));
		    							insertSQL= "INSERT INTO  location"+"(SISN,name,scope,coor,lat,lng)"+"VALUES"+
			    								"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC") +"\""+","+"\""+ scopeDes +"\""+","+"\""+ coor +"\""+","+ lat +","+ lng +")";
			    						//System.out.println(insertSQL);
			    						agtdb.runSql(insertSQL);
			    						break;
	    							}else if (lngWords.length==3){
		    							lng = lngWords[2].contains("W")? -1*((Float.parseFloat(lngWords[1])/60)+Float.parseFloat(lngWords[0])) : ((Float.parseFloat(lngWords[1])/60)+Float.parseFloat(lngWords[0]));
		    							insertSQL= "INSERT INTO  location"+"(SISN,name,scope,coor,lat,lng)"+"VALUES"+
			    								"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC") +"\""+","+"\""+ scopeDes +"\""+","+"\""+ coor +"\""+","+ lat +","+ lng +")";
			    						//System.out.println(insertSQL);
			    						agtdb.runSql(insertSQL);
			    						break;
	    							}
	    						}else if (latWords.length==3) {
	    							lat = latWords[2].contains("S")? -1*((Float.parseFloat(latWords[1])/60)+Float.parseFloat(latWords[0])) : ((Float.parseFloat(latWords[1])/60)+Float.parseFloat(latWords[0]));
	    							if (lngWords.length==4){
		    							lng = lngWords[3].contains("W")? -1*((((Float.parseFloat(lngWords[2])/60)+Float.parseFloat(lngWords[1]))/60)+Float.parseFloat(lngWords[0])) : ((((Float.parseFloat(lngWords[2])/60)+Float.parseFloat(lngWords[1]))/60)+Float.parseFloat(lngWords[0]));
		    							insertSQL= "INSERT INTO  location"+"(SISN,name,scope,coor,lat,lng)"+"VALUES"+
			    								"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC") +"\""+","+"\""+ scopeDes +"\""+","+"\""+ coor +"\""+","+ lat +","+ lng +")";
			    						//System.out.println(insertSQL);
			    						agtdb.runSql(insertSQL);
			    						break;
	    							}else if (lngWords.length==3){
		    							lng = lngWords[2].contains("W")? -1*((Float.parseFloat(lngWords[1])/60)+Float.parseFloat(lngWords[0])) : ((Float.parseFloat(lngWords[1])/60)+Float.parseFloat(lngWords[0]));
		    							insertSQL= "INSERT INTO  location"+"(SISN,name,scope,coor,lat,lng)"+"VALUES"+
			    								"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC") +"\""+","+"\""+ scopeDes +"\""+","+"\""+ coor +"\""+","+ lat +","+ lng +")";
			    						//System.out.println(insertSQL);
			    						agtdb.runSql(insertSQL);
			    						break;
	    							}
	    						}
	    						
	    					}else{
	    						scopeDes=scopeDes.replace("\"", "\\\"");
	    						insertSQL= "INSERT INTO  location"+"(SISN,name,scope)"+"VALUES"+
	    								"("+ rsetAgt.getInt("SISN") +","+ rsetAgt.getString("ENG_DESC") +","+ scopeDes +","+ ")";
	    						agtdb.runSql(insertSQL);
	    						content.append("SISN = "+rsetAgt.getInt("SISN")+"\n"+"Name = "+rsetAgt.getString("ENG_DESC")+"\n"+"ENG_SCOPE = "+rsetScope.getString("ENG_SCOPE")+"\n"+"\n");
	    					}
	    					
	    				}
	    			}while(rsetScope.next());
	    		}else{
	    			scopeDes=rsetScope.getString("ENG_SCOPE");
    				desWords=scopeDes.trim().split(" ");
    				if (desWords[0].contains("Location")){
    					matcher = pattern.matcher(scopeDes);
    					if (matcher.find()){    						
    						coor = matcher.group();
    						matlng = patlng.matcher(coor);
    						matlng.find();
    						longitude = matlng.group();
    						matlat = patlat.matcher(coor);
    						matlat.find();
    						latitude = matlat.group();
    						latWords = latitude.trim().split(" ");
    						lngWords = longitude.trim().split(" ");
    						scopeDes=scopeDes.replace("\"", "\\\"");
    						if (latWords.length==4){
    							lat = latWords[3].contains("S")? -1*((((Float.parseFloat(latWords[2])/60)+Float.parseFloat(latWords[1]))/60)+Float.parseFloat(latWords[0])) : ((((Float.parseFloat(latWords[2])/60)+Float.parseFloat(latWords[1]))/60)+Float.parseFloat(latWords[0]));
    							if (lngWords.length==4){
	    							lng = lngWords[3].contains("W")? -1*((((Float.parseFloat(lngWords[2])/60)+Float.parseFloat(lngWords[1]))/60)+Float.parseFloat(lngWords[0])) : ((((Float.parseFloat(lngWords[2])/60)+Float.parseFloat(lngWords[1]))/60)+Float.parseFloat(lngWords[0]));
	    							insertSQL= "INSERT INTO  location"+"(SISN,name,scope,coor,lat,lng)"+"VALUES"+
		    								"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC") +"\""+","+"\""+ scopeDes +"\""+","+"\""+ coor +"\""+","+ lat +","+ lng +")";
		    						//System.out.println(insertSQL);
		    						agtdb.runSql(insertSQL);
    							}else if (lngWords.length==3){
	    							lng = lngWords[2].contains("W")? -1*((Float.parseFloat(lngWords[1])/60)+Float.parseFloat(lngWords[0])) : ((Float.parseFloat(lngWords[1])/60)+Float.parseFloat(lngWords[0]));
	    							insertSQL= "INSERT INTO  location"+"(SISN,name,scope,coor,lat,lng)"+"VALUES"+
		    								"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC") +"\""+","+"\""+ scopeDes +"\""+","+"\""+ coor +"\""+","+ lat +","+ lng +")";
		    						//System.out.println(insertSQL);
		    						agtdb.runSql(insertSQL);
    							}
    						}else if (latWords.length==3) {
    							lat = latWords[2].contains("S")? -1*((Float.parseFloat(latWords[1])/60)+Float.parseFloat(latWords[0])) : ((Float.parseFloat(latWords[1])/60)+Float.parseFloat(latWords[0]));
    							if (lngWords.length==4){
	    							lng = lngWords[3].contains("W")? -1*((((Float.parseFloat(lngWords[2])/60)+Float.parseFloat(lngWords[1]))/60)+Float.parseFloat(lngWords[0])) : ((((Float.parseFloat(lngWords[2])/60)+Float.parseFloat(lngWords[1]))/60)+Float.parseFloat(lngWords[0]));
	    							insertSQL= "INSERT INTO  location"+"(SISN,name,scope,coor,lat,lng)"+"VALUES"+
		    								"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC") +"\""+","+"\""+ scopeDes +"\""+","+"\""+ coor +"\""+","+ lat +","+ lng +")";
		    						//System.out.println(insertSQL);
		    						agtdb.runSql(insertSQL);
    							}else if (lngWords.length==3){
	    							lng = lngWords[2].contains("W")? -1*((Float.parseFloat(lngWords[1])/60)+Float.parseFloat(lngWords[0])) : ((Float.parseFloat(lngWords[1])/60)+Float.parseFloat(lngWords[0]));
	    							insertSQL= "INSERT INTO  location"+"(SISN,name,scope,coor,lat,lng)"+"VALUES"+
		    								"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC") +"\""+","+"\""+ scopeDes +"\""+","+"\""+ coor +"\""+","+ lat +","+ lng +")";
		    						//System.out.println(insertSQL);
		    						agtdb.runSql(insertSQL);
    							}
    						}
    						
    					}else{
    						scopeDes=scopeDes.replace("\"", "\\\"");
    						insertSQL= "INSERT INTO  location"+"(SISN,name,scope)"+"VALUES"+
    								"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC") +"\""+","+"\""+ scopeDes +"\""+ ")";
    						//System.out.println(insertSQL);
    						agtdb.runSql(insertSQL);
    						content.append("SISN = "+rsetAgt.getInt("SISN")+"\n"+"Name = "+rsetAgt.getString("ENG_DESC")+"\n"+"ENG_SCOPE = "+rsetScope.getString("ENG_SCOPE")+"\n"+"\n");
    					}
    					
    				}
	    		}
	    		
	    	}
	    	
	    	
	    }
	   
		}
	    contentResult=content.toString();
	    out.append(contentResult);
	    out.flush();
		out.close();
		agtdb.finalize();
		
		System.out.println("Location Table First Part Done");

	}

		
	public void engColumParse ()  throws Throwable  {
		
		agtDB agtdb = new agtDB();
		String insertSQL;
		insertSQL = "SELECT SISN,ENG_DESC FROM agt";
		ResultSet rsetAgt= agtdb.runSqlResultSet(insertSQL);
		ResultSet rsetScope;
	    String scopeDes;
	    String latWords[],lngWords[];
	    Pattern pattern = Pattern.compile("\\d+\\s\\d+\\s(\\d+\\s)?[NS],\\s\\d+\\s\\d+\\s(\\d+\\s)?[WE]");   
	    Matcher matcher = pattern.matcher("");
	    Pattern patlng = Pattern.compile("\\d+\\s\\d+\\s(\\d+\\s)?[WE]");
	    Pattern patlat = Pattern.compile("\\d+\\s\\d+\\s(\\d+\\s)?[NS]");
	    Matcher matlng = patlng.matcher("");
	    Matcher matlat = patlat.matcher("");
	    String coor,longitude,latitude;
	    Float lng,lat;
	    System.out.println("Parse coordinate in Eng_colum Start");
		
	    while(rsetAgt.next()){
	    	insertSQL="SELECT * FROM location WHERE SISN = " + rsetAgt.getInt("SISN");
	    	rsetScope = agtdb.runSqlResultSet(insertSQL);
	    	scopeDes=rsetAgt.getString("ENG_DESC");
	    	//System.out.println(scopeDes);
	    	if (scopeDes != null){
	    	matcher = pattern.matcher(scopeDes);
	    	if (matcher.find() && rsetScope.next()==false){
	    		//count++;
	    		coor = matcher.group();
				matlng = patlng.matcher(coor);
				matlng.find();
				longitude = matlng.group();
				matlat = patlat.matcher(coor);
				matlat.find();
				latitude = matlat.group();
				latWords = latitude.trim().split(" ");
				lngWords = longitude.trim().split(" ");
				//System.out.println(rsetAgt.getInt("SISN") +"    "+ scopeDes +"   " + coor);
				if (latWords.length==4){
					lat = latWords[3].contains("S")? -1*((((Float.parseFloat(latWords[2])/60)+Float.parseFloat(latWords[1]))/60)+Float.parseFloat(latWords[0])) : ((((Float.parseFloat(latWords[2])/60)+Float.parseFloat(latWords[1]))/60)+Float.parseFloat(latWords[0]));
					if (lngWords.length==4){
						lng = lngWords[3].contains("W")? -1*((((Float.parseFloat(lngWords[2])/60)+Float.parseFloat(lngWords[1]))/60)+Float.parseFloat(lngWords[0])) : ((((Float.parseFloat(lngWords[2])/60)+Float.parseFloat(lngWords[1]))/60)+Float.parseFloat(lngWords[0]));
						insertSQL= "INSERT INTO  location"+"(SISN,name,scope,coor,lat,lng)"+"VALUES"+
								"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC") +"\""+","+"\""+ scopeDes +"\""+","+"\""+ coor +"\""+","+ lat +","+ lng +")";
						//System.out.println(insertSQL);
						agtdb.runSql(insertSQL);
					}else if (lngWords.length==3){
						lng = lngWords[2].contains("W")? -1*((Float.parseFloat(lngWords[1])/60)+Float.parseFloat(lngWords[0])) : ((Float.parseFloat(lngWords[1])/60)+Float.parseFloat(lngWords[0]));
						insertSQL= "INSERT INTO  location"+"(SISN,name,scope,coor,lat,lng)"+"VALUES"+
								"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC") +"\""+","+"\""+ scopeDes +"\""+","+"\""+ coor +"\""+","+ lat +","+ lng +")";
						//System.out.println(insertSQL);
						agtdb.runSql(insertSQL);
					}
				}else if (latWords.length==3) {
					lat = latWords[2].contains("S")? -1*((Float.parseFloat(latWords[1])/60)+Float.parseFloat(latWords[0])) : ((Float.parseFloat(latWords[1])/60)+Float.parseFloat(latWords[0]));
					if (lngWords.length==4){
						lng = lngWords[3].contains("W")? -1*((((Float.parseFloat(lngWords[2])/60)+Float.parseFloat(lngWords[1]))/60)+Float.parseFloat(lngWords[0])) : ((((Float.parseFloat(lngWords[2])/60)+Float.parseFloat(lngWords[1]))/60)+Float.parseFloat(lngWords[0]));
						insertSQL= "INSERT INTO  location"+"(SISN,name,scope,coor,lat,lng)"+"VALUES"+
								"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC") +"\""+","+"\""+ scopeDes +"\""+","+"\""+ coor +"\""+","+ lat +","+ lng +")";
						//System.out.println(insertSQL);
						agtdb.runSql(insertSQL);
					}else if (lngWords.length==3){
						lng = lngWords[2].contains("W")? -1*((Float.parseFloat(lngWords[1])/60)+Float.parseFloat(lngWords[0])) : ((Float.parseFloat(lngWords[1])/60)+Float.parseFloat(lngWords[0]));
						insertSQL= "INSERT INTO  location"+"(SISN,name,scope,coor,lat,lng)"+"VALUES"+
								"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC") +"\""+","+"\""+ scopeDes +"\""+","+"\""+ coor +"\""+","+ lat +","+ lng +")";
						//System.out.println(insertSQL);
						agtdb.runSql(insertSQL);
					}
				}
				
	    	}
	    }
	    }
	    System.out.println("Location Table Second Part done");
	    agtdb.finalize();

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
	    System.out.println("Geoname setting Start");
	    File logfile = new File("log.txt");
		Writer logout = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logfile,true), "UTF8"));
		logout.append(Calendar.getInstance().getTime() + " Geoname setting Start \n");
		logout.flush();
	    
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
	    			//System.out.println(rsetAgt.getString("ENG_DESC"));
	    			agtdb.runSql(insertSQL);
	    			
	    		}else{
    				geoUrl = "http://api.geonames.org/searchJSON?q=" + URLEncoder.encode(rsetAgt.getString("ENG_DESC").replace("region",""), "UTF-8") + "&username="+userName;
    				//System.out.println(geoUrl);
    				output = connectToPage(geoUrl);
    				geoObject=(JsonObject)new JsonParser().parse(output);
    				resultCount = geoObject.get("totalResultsCount").getAsInt();
    				geonames = geoObject.get("geonames").getAsJsonArray();
    				if (resultCount == 0){
    					insertSQL= "INSERT INTO  geonames"+"(id,name,geoYN)"+"VALUES"+
    							"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC")+"\""+","+"\""+"O"+"\""+")";
    					//System.out.println(rsetAgt.getString("ENG_DESC"));
    					agtdb.runSql(insertSQL);
    				}else {
    					firstObj=geonames.get(0).getAsJsonObject();
    					lat=firstObj.get("lat").getAsFloat();
    					lng=firstObj.get("lng").getAsFloat();
    					insertSQL= "INSERT INTO  geonames"+"(id,name,lat,lng,geoYN)"+"VALUES"+
    								"("+ rsetAgt.getInt("SISN") +","+"\""+ rsetAgt.getString("ENG_DESC")+"\""+","+lat+","+lng+","+"\""+"Y"+"\""+")";
    					//System.out.println(rsetAgt.getString("ENG_DESC"));
    					agtdb.runSql(insertSQL);
    					
    				}
    			}
	    	}
	    		 	
	    }
	    
	    System.out.println("Geoname Setting Done");
	    logout.append(Calendar.getInstance().getTime() + " Geoname Setting Done \n");
		logout.flush();
	    logout.close();
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
