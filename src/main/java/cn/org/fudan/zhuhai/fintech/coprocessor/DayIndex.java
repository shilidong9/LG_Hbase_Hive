package cn.org.fudan.zhuhai.fintech.coprocessor;

import java.io.IOException;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
/**
 * hbase 1系列版本
 */
public class DayIndex extends BaseRegionObserver {
	static Connection connection = null;
	static Table goodTable = null;
	static Table badTable = null;
	static Table histryTable = null;

	static {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.17.27.116:2181,72.17.27.117:2181,172.17.27.118:2181");

		try {
			connection = ConnectionFactory.createConnection(conf);
			histryTable = connection.getTable(TableName.valueOf("mcu_history"));
			goodTable = connection.getTable(TableName.valueOf("mcu_history_index"));
			badTable = connection.getTable(TableName.valueOf("mcu_history_bad_index"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	@SuppressWarnings("unused")
	private RegionCoprocessorEnvironment env = null;

	// 协处理器是运行于region中的，每一个region都会加载协处理器
	// 这个方法会在regionserver打开region时候执行（还没有真正打开）
	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		env = (RegionCoprocessorEnvironment) e;
	}

	// 这个方法会在regionserver关闭region时候执行（还没有真正关闭）
	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		// nothing to do here
	}

	public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put, final WALEdit edit,
			final Durability durability) throws IOException {
		HashMap<String, ArrayList<String>> filterMap = new HashMap<String, ArrayList<String>>();
		ArrayList<String> al = new ArrayList<String>();
		String hexString = "0123456789ABCDEF";
		String filterQuality = "MCU_STREAM_DATA";
		Boolean badData = false;
		al.add("TOTALFUEL");
		al.add("ENGINEHOURS");
		filterMap.put("F986", al);
		filterMap.put("F883", al);

		al = new ArrayList<String>();
		al.add("TOTALFUEL");
		al.add("ENGINEHOURS");
		al.add("SINGLEFUEL");
		filterMap.put("F881", al);
		filterMap.put("F880", al);

		al = new ArrayList<String>();
		al.add("ENGINEHOURS");
		filterMap.put("FB80", al);
		filterMap.put("FB81", al);
		filterMap.put("02D0", al);

		al = new ArrayList<String>();
		al.add("ENGINEHOURS");
		al.add("TOTALFUELCONSUMPTION");
		filterMap.put("F980", al);

		al = new ArrayList<String>();
		al.add("LOWERENGINEHOURS");
		al.add("LOWERTOTALFUEL");
		filterMap.put("FB83", al);
		byte[] row = put.getRow();
		String rawKey = new String(row);
		String type = "";

		try {
			byte[] valueBytes = CellUtil.cloneValue(put.get(Bytes.toBytes("d"), Bytes.toBytes(filterQuality)).get(0));
			StringBuilder sb = new StringBuilder(valueBytes.length * 2);
			for (int i = 0; i < valueBytes.length; i++) {
				sb.append(hexString.charAt((valueBytes[i] & 0xf0) >> 4));
				sb.append(hexString.charAt((valueBytes[i] & 0x0f) >> 0));
			}

			type = sb.toString();
			if (type.startsWith("8101") || type.startsWith("00000000")) {
				String pre = type.substring(8, 10);
				String con = type.substring(10, 12);
				type = con + pre;
			}
			if (type.startsWith("2323")) {
				type = type.substring(5, 9);
			}
			if (type.startsWith("7E")) {
				type = type.substring(3, 7);
			}
		} catch (Exception e1) {
		}
		try {
			for (String ql : filterMap.get(type)) {
				String filterV = new String(
						CellUtil.cloneValue(put.get(Bytes.toBytes("d"), Bytes.toBytes((String) ql)).get(0)));
				if (filterV.isEmpty()) {
					badData = true;
					break;
				} else {
					String usage = String.valueOf(filterV);
					if (usage.compareTo("0") <= 0) {
						badData = true;
						break;
					}
				}

			}
		} catch (Exception e1) {
		}
		String timeKey = rawKey.split("-")[1];
		Long tamstap = Long.MAX_VALUE - Long.valueOf(timeKey);
		String timeIndex = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(tamstap);
		String rowIndex = timeIndex + "-" + rawKey;
		Put putIndex = new Put(Bytes.toBytes(rowIndex));
		putIndex.addColumn(Bytes.toBytes("d"), Bytes.toBytes("k"), Bytes.toBytes(""));
		if (badData == false) {
			goodTable.put(putIndex);
		} else {
			badTable.put(putIndex);
		}
		badData = false;

	}
}
