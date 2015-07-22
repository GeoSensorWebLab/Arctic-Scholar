package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Data {
	
	public void Generation(String fileName, String insertSQL) throws SQLException, Exception, Throwable {
		
		try {
			
			astisDB astisdb = new astisDB();
			agtDB agtdb = new agtDB();
			
			StringBuilder content = new StringBuilder();
			String contentResult = "";
			
			File file = new File(fileName);
			
			Writer out = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(file), "UTF8"));
			
			
			ResultSet rsetAstis= astisdb.runSqlResultSet(insertSQL);
			ResultSetMetaData mdataAstis = rsetAstis.getMetaData();
			
			ResultSet rsetTable;
			ResultSetMetaData mdataTable;
			ResultSet rsetAgt;
			ResultSetMetaData mdataAgt;
			Long start_time;
			boolean checkAgt = false;
			
			while(rsetAstis.next()){
				System.out.println(rsetAstis.getObject(1));
				start_time = System.nanoTime();
				content.setLength(0);
				content.append("{\"index\":{\"_id\":\""+rsetAstis.getObject(1)+"\"}}\n{");
				content.append( "\""+mdataAstis.getColumnName(1)+"\":"+ rsetAstis.getObject(1)+",");
				for (int i =2; i<=mdataAstis.getColumnCount(); i++){
					if(String.valueOf(rsetAstis.getObject(i))=="null"){
						content.append( "\""+mdataAstis.getColumnName(i)+"\":\""+ rsetAstis.getObject(i)+"\",");
						//System.out.println("A  "+mdataAstis.getColumnName(i)+ "  "+rsetAstis.getObject(i));
					}else {
						content.append( "\""+mdataAstis.getColumnName(i)+"\":\""+ rsetAstis.getObject(i).toString().replace("\\", "\\\\").replace("\"","\\\"")+"\",");
					}
				}
				
				insertSQL = "SELECT * FROM astis_ai WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+mdataTable.getColumnName(2)+"\":[");
					do{
						content .append( "\""+ rsetTable.getObject(2)+ "\""+ ",");
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				
				
				
				insertSQL = "SELECT * FROM astis_aut WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+"Aut"+"\":[" );
					do{
						content.append( "{" );
						for (int i=2; i<=mdataTable.getColumnCount(); i++){
							if(String.valueOf(rsetTable.getObject(i))=="null"){
								content.append( "\""+ mdataTable.getColumnName(i)+"\":\""+ rsetTable.getObject(i)+ "\""+ ",");
							}else{
								content.append( "\""+ mdataTable.getColumnName(i)+"\":\""+ rsetTable.getObject(i).toString().replace("\\", "\\\\").replace("\"","\\\"")+ "\""+ ",");
							}
							
						}
						content.delete(content.length()-1,content.length());
						content.append("},");
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				
				insertSQL = "SELECT * FROM astis_bg WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+mdataTable.getColumnName(2)+"\":[");
					do{
						content.append( "\""+ rsetTable.getObject(2)+ "\""+ ",");
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				
				insertSQL = "SELECT * FROM astis_bi WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+mdataTable.getColumnName(2)+"\":[");
					do{
						content.append( "\""+ rsetTable.getObject(2)+ "\""+ ",");
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				
				insertSQL = "SELECT * FROM astis_bn WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+mdataTable.getColumnName(2)+"\":[");
					do{
						content.append( "\""+ rsetTable.getObject(2)+ "\""+ ",");
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				
				insertSQL = "SELECT * FROM astis_bs WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+mdataTable.getColumnName(2)+"\":[");
					do{
						content.append( "\""+ rsetTable.getObject(2)+ "\""+ ",");
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				
				insertSQL = "SELECT * FROM astis_dat WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+mdataTable.getColumnName(2)+"\":[");
					do{
						content.append( "\""+ rsetTable.getObject(2)+ "\""+ ",");
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				
				insertSQL = "SELECT * FROM astis_do WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+mdataTable.getColumnName(2)+"\":[");
					do{
						content.append( "\""+ rsetTable.getObject(2)+ "\""+ ",");
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				
				insertSQL = "SELECT * FROM astis_gh WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				while (rsetTable.next()){
					insertSQL = "SELECT * FROM geonames WHERE name = \"" + rsetTable.getObject(2) + "\";";
					rsetAgt = agtdb.runSqlResultSet(insertSQL);
					if (rsetAgt.next() && checkAgt == false){
						content.append("\""+"location"+"\":{\"type\":\"multipoint\",\"coordinates\":[");
						checkAgt = true;
					}
					if (checkAgt == true && rsetAgt.first()==true){
						content.append("["+rsetAgt.getObject(4)+","+rsetAgt.getObject(3)+"],");
					}
				}
				if (checkAgt == true){
					content.delete(content.length()-1,content.length());
					content.append("]},");
					checkAgt = false;
				}
				
				
				insertSQL = "SELECT * FROM astis_gh WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+ "gh" +"\":[");
					do{
						content.append( "{");
						content.append( "\""+ mdataTable.getColumnName(2)+"\":\""+ rsetTable.getObject(2)+"\",");
						insertSQL = "SELECT * FROM agt WHERE ENG_DESC = \"" + rsetTable.getObject(2) + "\";";
						//System.out.println(insertSQL);
						rsetAgt = agtdb.runSqlResultSet(insertSQL);
						if (rsetAgt.next()){
							insertSQL = "SELECT * FROM agt_eng_scope WHERE SISN = " + rsetAgt.getObject(1)+";";
							//System.out.println(insertSQL);
							rsetAgt = agtdb.runSqlResultSet(insertSQL);
							if (rsetAgt.next()){
								mdataAgt = rsetAgt.getMetaData();
								if(String.valueOf(rsetAgt.getObject(2))=="null"){
									content.append( "\""+ mdataAgt.getColumnName(2)+"\":\""+ rsetAgt.getObject(2)+"\"},");
								}else{
									content.append( "\""+ mdataAgt.getColumnName(2)+"\":\""+ rsetAgt.getObject(2).toString().replace("\\", "\\\\").replace("\"","\\\"")+"\"},");
								}
								
							}else{
								content.delete(content.length()-1,content.length());
								content.append("},");
							}
						}else{
							content.delete(content.length()-1,content.length());
							content.append("},");	
						}
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				
				
				insertSQL = "SELECT * FROM astis_lib WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+"lib"+"\":[");
					do{
						content.append( "{");
						for (int i=2; i<=mdataTable.getColumnCount(); i++){
							content.append( "\""+ mdataTable.getColumnName(i)+"\":\""+ rsetTable.getObject(i)+ "\""+ ",");
						}
						content.delete(content.length()-1,content.length());
						content.append("},");
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				
				insertSQL = "SELECT * FROM astis_me WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+mdataTable.getColumnName(2)+"\":[");
					do{
						if (String.valueOf(rsetTable.getObject(2))=="null"){
							content.append( "\""+ rsetTable.getObject(2)+ "\""+ ",");
						}else{
							content.append( "\""+ rsetTable.getObject(2).toString().replace("\\", "\\\\").replace("\"","\\\"")+ "\""+ ",");
						}	
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				
				insertSQL = "SELECT * FROM astis_no WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+mdataTable.getColumnName(2)+"\":[");
					do{
						if(String.valueOf(rsetTable.getObject(2))=="null"){
							content.append( "\""+ rsetTable.getObject(2) + "\""+ ",");
						}else{
							content.append( "\""+ rsetTable.getObject(2).toString().replace("\\", "\\\\").replace("\"","\\\"") + "\""+ ",");
						}
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				
				insertSQL = "SELECT * FROM astis_ser WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+"ser"+"\":[");
					do{
						content.append( "{");
						for (int i=2; i<=mdataTable.getColumnCount(); i++){
							if(String.valueOf(rsetTable.getObject(i))=="null"){
								content.append( "\""+ mdataTable.getColumnName(i)+"\":\""+ rsetTable.getObject(i)+ "\""+ ",");
							}else {
								content.append( "\""+ mdataTable.getColumnName(i)+"\":\""+ rsetTable.getObject(i).toString().replace("\\", "\\\\").replace("\"","\\\"")+ "\""+ ",");
							}
						}
						content.delete(content.length()-1,content.length());
						content.append("},");
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				
				insertSQL = "SELECT * FROM astis_sh WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+mdataTable.getColumnName(2)+"\":[");
					do{
						if(String.valueOf(rsetTable.getObject(2))=="null"){
							content.append( "\""+ rsetTable.getObject(2)+ "\""+ ",");
						}else{
							content.append( "\""+ rsetTable.getObject(2).toString().replace("\\", "\\\\").replace("\"","\\\"")+ "\""+ ",");
						}
						
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				
				insertSQL = "SELECT * FROM astis_sn WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+mdataTable.getColumnName(2)+"\":[");
					do{
						content.append( "\""+ rsetTable.getObject(2)+ "\""+ ",");
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				
				insertSQL = "SELECT * FROM astis_so WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+mdataTable.getColumnName(2)+"\":[");
					do{
						content.append( "\""+ rsetTable.getObject(2)+ "\""+ ",");
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				
				insertSQL = "SELECT * FROM astis_url WHERE SISN = " + rsetAstis.getObject(1) + ";";
				rsetTable = astisdb.runSqlResultSet(insertSQL);
				if (rsetTable.next()){
					mdataTable = rsetTable.getMetaData();
					content.append( "\""+"url"+"\":[");
					do{
						content.append( "{");
						for (int i=2; i<=mdataTable.getColumnCount(); i++){
							content.append( "\""+ mdataTable.getColumnName(i)+"\":\""+ rsetTable.getObject(i)+ "\""+ ",");
						}
						content.delete(content.length()-1,content.length());
						content.append("},");
					}while(rsetTable.next());
					content.delete(content.length()-1,content.length());
					content.append("],");
				}
				content.delete(content.length()-1,content.length());
				content.append("}\n");
				contentResult = content.toString();
				out.append(contentResult);
				System.out.println((System.nanoTime() - start_time)/1000000000.0);
				
			}
			
			
			out.flush();
			out.close();
			
			agtdb.finalize();
			astisdb.finalize();
			

			//System.out.println("Done");

		} 
		catch (UnsupportedEncodingException e) 
		{
			e.printStackTrace();
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
	}
	
	public void fileDeletion(String fileName){
		try{
    		File file = new File(fileName);
    		if(file.delete()){
    			System.out.println(file.getName() + " is deleted!");
    		}else{
    			System.out.println("Delete operation is failed.");
    		}
    	}catch(Exception e){
    		e.printStackTrace();
    	}
	}
	
	public void dataBatch (String EsSite, String fileName, String exceptionFileName) throws Throwable {
		
		JsonObject esRespond=new JsonObject();
		JsonArray esItems=new JsonArray();	
		String command = "curl -XPOST " + EsSite + " --data-binary @" + fileName ;
		//System.out.println(command);
		String output = executeCommand(command);
		//System.out.println(output);
		String excepOutput = "";
		String insertSQL = "";
		
		File file = new File(exceptionFileName);
		Writer exception = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,true), "UTF8"));
		
		esRespond=(JsonObject)new JsonParser().parse(output);
		esItems = esRespond.get("items").getAsJsonArray();
		
		for (int i=0;i < esItems.size();i++){
			if(esItems.get(i).getAsJsonObject().get("index").getAsJsonObject().get("status").getAsInt()!=200 && esItems.get(i).getAsJsonObject().get("index").getAsJsonObject().get("status").getAsInt()!=201 ){
				insertSQL = "SELECT * FROM astis WHERE SISN = " + esItems.get(i).getAsJsonObject().get("index").getAsJsonObject().get("_id").getAsInt();
				//System.out.println(insertSQL);
				excepOutput = exceptionDataRunning.main(insertSQL);
				exception.append(excepOutput);
			}
		}
		exception.flush();
		exception.close();
	}
	
	
	public String executeCommand(String command) {
		 
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
