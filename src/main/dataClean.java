package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.StringBuilder;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class dataClean {

	public static void main(String[] args) throws SQLException, Exception, Throwable {
		
		Properties prop = new Properties();
		InputStream input = null;
		String fileName = "";
		String EsSite = "";
		String exceptFileName = "";
		int batchNum = 0;
		
		try {
			 
			input = new FileInputStream("config.properties");
			prop.load(input);
			batchNum = Integer.parseInt(prop.getProperty("batchNum"));
			fileName = prop.getProperty("tempGenrateDataName");
			EsSite = prop.getProperty("EsSite");
			exceptFileName = prop.getProperty("exceptionFileName");
	 
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
		
		
		String excepData = "";
	    excepData = fileReading(exceptFileName);
	    excepData = excepData.replace("", "").replace("", "");
	    
	    if(excepData.isEmpty() == false){
	    	batchExcepData (EsSite,fileName,exceptFileName, excepData, batchNum);
	    }
		//data.fileDeletion(fileName);
		
	}
	
	
	public static void batchExcepData (String EsSite, String fileName, String exceptFileName, String excepData, int batchNum)throws SQLException, Exception, Throwable{
		Pattern pattern = Pattern.compile("\\{\"index\":\\{\"_id\":\"(\\d+)\"\\}\\}");   
		Matcher matcher = pattern.matcher("");
		matcher = pattern.matcher(excepData);
		
		File exceptFile = new File(exceptFileName);
		Writer exceptOut = new BufferedWriter(new OutputStreamWriter(
		new FileOutputStream(exceptFile), "UTF8"));
		exceptOut.close();
		
		File file = new File(fileName);
		Writer out = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(file), "UTF8"));
		
		StringBuilder content = new StringBuilder(); 
	    while (matcher.find()){
	    	excepData = excepData.replace(matcher.group(),","+matcher.group()+",");
	    }
	    excepData=excepData.substring(1,excepData.length());
	    JsonArray jsonExcept = (JsonArray)new JsonParser().parse("["+excepData+"]");
	    System.out.println(jsonExcept.size());
	    
	    for (int i=0;i<(jsonExcept.size()/(batchNum*2))+1;i++){
	    	content.delete(0,content.length());
	    	out = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(file), "UTF8"));
	    	int temp=i*batchNum*2;
	    	int j=0;
	    	for (j=0;j<batchNum*2;j+=2){
	    		if ((jsonExcept.size()-(temp+j))>0){
	    			content.append(jsonExcept.get(temp+j)+"\n");
	    			content.append(jsonExcept.get(temp+j+1)+"\n");
	    		}else{
	    			break;
	    		}
	    	}
	    	
	    	out.append(content.toString());
	    	out.flush();
	        out.close();
	        dataBatch (EsSite,fileName, exceptFileName);
	    }
	}
	
	public static void dataBatch (String EsSite, String fileName, String exceptFileName) throws Throwable {
		
		JsonObject esRespond=new JsonObject();
		JsonArray esItems=new JsonArray();	
		String command = "curl -XPOST " + EsSite + " --data-binary @" + fileName ;
		//System.out.println(command);
		//Long start_time;
		//start_time = System.nanoTime();
		String output = executeCommand(command);
		//System.out.println((System.nanoTime() - start_time)/1000000000.0);
		//System.out.println(output);
		String excepOutput = "";
		String insertSQL = "";
		
		File file = new File(exceptFileName);
		Writer exception = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,true), "UTF8"));
		
		esRespond=(JsonObject)new JsonParser().parse(output);
		esItems = esRespond.get("items").getAsJsonArray();
		
		for (int i=0;i < esItems.size();i++){
			if(esItems.get(i).getAsJsonObject().get("index").getAsJsonObject().get("status").getAsInt()!=200 && esItems.get(i).getAsJsonObject().get("index").getAsJsonObject().get("status").getAsInt()!=201 ){
				insertSQL = "SELECT * FROM astis WHERE SISN = " + esItems.get(i).getAsJsonObject().get("index").getAsJsonObject().get("_id").getAsInt();
				System.out.println(insertSQL);
				excepOutput = exceptionDataRunning.main(insertSQL);
				exception.append(excepOutput);
			}
		}
		exception.flush();
		exception.close();
	}
	
	public static String fileReading(String fileDir){
		String output="";
		BufferedReader br = null;
		String tempString = "";
		 
		try {
 
			StringBuilder content = new StringBuilder();
			content.append("");
			br = new BufferedReader(new FileReader(fileDir));
 
			while ((tempString = br.readLine()) != null) {
				content.append(tempString+"\n");
			}
			output = content.toString();
			
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return output;
	}
	
	public static String executeCommand(String command) {
		 
		StringBuffer output = new StringBuffer();
 
		Process p;
		try {
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
			BufferedReader reader = 
                            new BufferedReader(new InputStreamReader(p.getInputStream()));
 
                        String line = "";			
			while ((line = reader.readLine())!= null) {
				output.append(line + "\n");
			}
 
		} catch (Exception e) {
			e.printStackTrace();
		}
 
		return output.toString();
 
	}

}
