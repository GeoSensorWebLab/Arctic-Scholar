package main;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.lang.StringBuilder;


public class exceptionDataRunning {

	
	public static String main(String insertSQL) throws SQLException, Exception {
		
		astisDB astisdb = new astisDB();
		agtDB agtdb = new agtDB();
		
		String output="";
		StringBuilder content = new StringBuilder();
		
		ResultSet rsetAstis= astisdb.runSqlResultSet(insertSQL);
		ResultSetMetaData mdataAstis = rsetAstis.getMetaData();
		
		ResultSet rsetTable;
		ResultSetMetaData mdataTable;
		ResultSet rsetAgt;
		ResultSetMetaData mdataAgt;
		//Long start_time;
		
		while(rsetAstis.next()){
			System.out.println(rsetAstis.getObject(1));
			//start_time = System.nanoTime();
			content.setLength(0);
			content.append("{\"index\":{\"_id\":\""+rsetAstis.getObject(1)+"\"}}\n{");
			content.append( "\""+mdataAstis.getColumnName(1)+"\":"+ rsetAstis.getObject(1)+",");
			for (int i =2; i<=mdataAstis.getColumnCount(); i++){
				if(String.valueOf(rsetAstis.getObject(i))=="null"){
					content.append( "\""+mdataAstis.getColumnName(i)+"\":\""+ rsetAstis.getObject(i)+"\",");
				}else {
					content.append( "\""+mdataAstis.getColumnName(i)+"\":\""+ rsetAstis.getObject(i).toString().replace("\"","\\\"").replace("\\ ", "\\\\ ")+"\",");
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
							content.append( "\""+ mdataTable.getColumnName(i)+"\":\""+ rsetTable.getObject(i).toString().replace("\"","\\\"").replace("\\ ", "\\\\ ")+ "\""+ ",");
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
			if (rsetTable.next()){
				insertSQL = "SELECT * FROM geonames WHERE name = \"" + rsetTable.getObject(2) + "\";";
				rsetAgt = agtdb.runSqlResultSet(insertSQL);
				if (rsetAgt.next()){
					content.append( "\""+"location"+"\":{\"type\":\"point\",\"coordinates\":["+rsetAgt.getObject(4)+","+rsetAgt.getObject(3)+"]},");
				}
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
							}else {
								content.append( "\""+ mdataAgt.getColumnName(2)+"\":\""+ rsetAgt.getObject(2).toString().replace("\"","\\\"")+"\"},");
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
						content.append( "\""+ rsetTable.getObject(2).toString().replace("\"","\\\"")+ "\""+ ",");
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
						content.append( "\""+ rsetTable.getObject(2).toString().replace("\"","\\\"") + "\""+ ",");
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
						}else{
							content.append( "\""+ mdataTable.getColumnName(i)+"\":\""+ rsetTable.getObject(i).toString().replace("\"","\\\"")+ "\""+ ",");
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
					content.append( "\""+ rsetTable.getObject(2)+ "\""+ ",");
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
			output = content.toString();
		
			//System.out.println((System.nanoTime() - start_time)/1000000000.0);
		}
		
		//System.out.println("ExcetionDataRunning Done");

		return(output);
	}

}
