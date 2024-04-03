package com.example.demo.listeners;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

public class DatabaseConnectivity {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public DatabaseConnectivity(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public static void main(String[] args) {
		List<Map<String, String>> assembData = new ArrayList<>();
		JdbcTemplate jdbcTemplate = new JdbcTemplate();
		  DatabaseConnectivity databaseConnectivity = new DatabaseConnectivity(jdbcTemplate);
		    
		// Row 1
		Map<String, String> row1 = new HashMap<>();
		row1.put("ASSEMB", "1199J114BULK");
		row1.put("LINENO", "1");
		row1.put("COMPNT", "1199J114");
		row1.put("COMST", "hhhh");
		row1.put("COMQTY", "fgwsgshw");
		row1.put("CRTDAT", "ghjjgjj");
		row1.put("REVDAT", "jhghjghgj");
		row1.put("REVNUM", "jjhjgds");
		assembData.add(row1);
		Map<String, String> row2 = new HashMap<>();
		row2.put("ASSEMB", "1199K115BULK");
		row2.put("LINENO", "1");
		row2.put("COMPNT", "1199K115");
		row2.put("COMST", "gjgjgjgj");
		row2.put("COMQTY", "hghgwdh");
		row2.put("CRTDAT", "fdfghj");
		row2.put("REVDAT", "hjhghgj");
		row2.put("REVNUM", "wsdwdsd");
		assembData.add(row2);
		Map<String, String> row3 = new HashMap<>();
		row3.put("ASSEMB", "1199M117BULK");
		row3.put("LINENO", "1");
		row3.put("COMPNT", "1199M117");
		row3.put("COMST", "kkkk");
		row3.put("COMQTY", "gjjdgsjd");
		row3.put("CRTDAT", "hghjujk");
		row3.put("REVDAT", "hgjhgjh");
		row3.put("REVNUM", "dds");
		assembData.add(row3);
		String idColName = "assemb";
		String tableName = "bill_of_materials";
		// Establish database connection
		List<String> allUpsertQuery = new ArrayList<>();
		for (Map<String, String> keyValueMap : assembData) {
			String upsertQuery = buildUpsertQuery(tableName, idColName, keyValueMap);
			System.out.println("Upsert Query -  "+upsertQuery);
			//allUpsertQuery.add(upsertQuery);
		}
		//databaseConnectivity.executeBatchUpsertQueries(allUpsertQuery);
	}

	private static String buildUpsertQuery(String tableName, String idColName, Map<String, String> queryData) {
		// Print the deserialized data
		System.out.println("Deserialized message:");
		StringBuilder queryBuilder = new StringBuilder("INSERT INTO " + tableName + "(");
		StringBuilder valuesBuilder = new StringBuilder("VALUES (");
		StringBuilder conflictBuilder = new StringBuilder("ON CONFLICT (" + idColName + ") DO UPDATE SET ");
		StringBuilder whereBuilder = new StringBuilder("WHERE");
		boolean firstEntry = true;
		for (Map.Entry<String, String> entry : queryData.entrySet()) {
			if (!firstEntry) {
				queryBuilder.append(", ");
				valuesBuilder.append(", ");
				conflictBuilder.append(", ");
				whereBuilder.append("OR");
			} else {
				firstEntry = false;
			}
			queryBuilder.append(entry.getKey()); // Append column name
			valuesBuilder.append("'").append(entry.getValue()).append("'");
			conflictBuilder
					.append(entry.getKey() + " = CASE WHEN" + tableName + "." + entry.getKey() + " <> " + entry.getValue()
							+ " THEN " + entry.getValue() + " ELSE " + tableName + "." + entry.getKey() + "END");
			whereBuilder.append(tableName + "." + entry.getKey() + " <> " + entry.getValue());
		}
		queryBuilder.append(",created_date,created_by )");
		valuesBuilder.append(",CURRENT_TIMESTAMP,'@System')"); // Close the VALUES part
		queryBuilder.append(valuesBuilder.toString());
		conflictBuilder.append("updated_date = CURRENT_TIMESTAMP,updated_by=@System");
		queryBuilder.append(conflictBuilder.toString());
		queryBuilder.append(whereBuilder.toString());
		return queryBuilder.toString();

	}
	
	@Transactional(rollbackFor = Exception.class)
	public void executeBatchUpsertQueries(List<String> upsertQueries) {
	    try {
	        jdbcTemplate.batchUpdate(upsertQueries.toArray(new String[0]));
	    } catch (Exception e) {
	    	  System.err.println("UpSertion Fails ");
	        sendFailedQueriesToKafka(upsertQueries);
	        // Optionally, log the exception or perform other error handling
	    }
	}
	   private void sendFailedQueriesToKafka(List<String> failedQueries) {
		   System.err.println("Failed Querys Reached to the Kafka ");
	            //kafkaTemplate.send(kafkaTopic, query);
	    }
}
