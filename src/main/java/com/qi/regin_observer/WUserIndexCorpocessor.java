package com.qi.regin_observer;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

public class WUserIndexCorpocessor extends BaseRegionObserver{

	private Connection connection;
	private Configuration configuration;
	
	private Table userIndex;
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
			throws IOException {
		if (configuration==null) {
			configuration=e.getEnvironment().getConfiguration();
		}
		if (connection==null) {
			connection=ConnectionFactory.createConnection(configuration);
		}
		if (userIndex==null) {
			userIndex=connection.getTable(TableName.valueOf("bd20:w_user_name_index"));
		}
		
		List<Cell> cells = put.get(Bytes.toBytes("i"), Bytes.toBytes("name"));   
		
		if (cells!=null && cells.size()>0) {
			byte[] name = CellUtil.cloneValue(cells.get(0));
			byte[] rowKey = CellUtil.cloneRow(cells.get(0));
			Put indexPut=new Put(name);
			indexPut.addColumn(Bytes.toBytes("i"), rowKey, null);
			userIndex.put(indexPut);
		}
		    
	}
}
