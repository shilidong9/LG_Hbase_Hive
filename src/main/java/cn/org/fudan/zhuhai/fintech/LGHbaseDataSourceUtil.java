package cn.org.fudan.zhuhai.fintech;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.hbase.filter.*; 


/***
 * 使用Java访问HBase
 * 
 *
 */
public class LGHbaseDataSourceUtil
{
	private static Configuration conf = null;
	//配置文件
	private static Admin admin = null;
	//管理员
	private static  Connection conn = null;
	//连接
	private static ThreadLocal<List<Put>> threadLocal = new ThreadLocal<List<Put>>();
	/**
	 * 初始化连接
	 * @throws IOException
	 */
	public static void init() throws IOException
	{
		System.setProperty("hadoop.home.dir", "c:\\hadoop-3.1.4"); 
		// 必备条件之一
		//运行这个程序的计算机都要配置hadoop
		conf = HBaseConfiguration.create();
		//创建一个配置
	//	conf.set("hbase.zookeeper.quorum", "192.168.3.121:2181,192.168.3.123:2181,192.168.3.124:2181"); 
		conf.set("hbase.zookeeper.quorum", "172.17.27.116:2181,72.17.27.117:2181,172.17.27.118:2181"); //测试
  	//  conf.set("hbase.zookeeper.quorum", "172.17.27.113:2181,72.17.27.107:2181,172.17.27.110:2181"); 	//生产
		//设置zookeeper的结点主机名
		conn = ConnectionFactory.createConnection(conf);
		//使用配置创建一个HBase连接
		admin = conn.getAdmin();
		//得到管理器
	}

	/**
	 * scan 'liugong-test',{COLUMNS=>'machineNo:C0193088',LIMIT=>2} 
	 * 浏览表中某一列(族)的数据。
	 * 如果该列族有若干列限定符，就列出每个列限定符代表的列的数据； 如果列名以“columnFamily:column”形式给出，只需列出该列的数据。
	 * 
	 * @param tableName 表名
	 * @param column    列名
	 */
	public static void scanColumn(String tableName, String column) throws IOException {
		HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
		String[] split = column.split(":");
		// 首先判断“列名”的类型，即是“columnFamily”还是“columnFamily:column”
		// 如果是“columnFamily”
		if (split.length == 1) {
			ResultScanner scanner = table.getScanner(column.getBytes());
			for (Result result : scanner) {
				// 获取该列族的所有的列限定符，存放在 cols 中
				Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(column));
				ArrayList<String> cols = new ArrayList<String>();
				for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
					cols.add(Bytes.toString(entry.getKey()));
				}
				// 循环便利，将所有列限定符的值都输出
				for (String str : cols) {
					System.out.print(str + ":" + new String(result.getValue(column.getBytes(), str.getBytes())) + " | ");
				}
				System.out.println();
			}
			// 释放扫描器
			scanner.close();
		} else {
			// 如果是“columnFamily:column”
			ResultScanner scanner = table.getScanner(split[0].getBytes(), split[1].getBytes());
			for (Result result : scanner) {
				System.out.println(new String(result.getValue(split[0].getBytes(), split[1].getBytes())));
			}
			// 释放扫描器
			scanner.close();
		}
		table.close();
	}
	
	

	public static List<Map.Entry> getRangeData(String tableName, String startRowKey, String endRowKey,String machineNo) {
		HTable hTable = null;
		List<Map.Entry> list = new ArrayList<>();
		try {
			hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(startRowKey));
			scan.setStopRow(Bytes.toBytes(endRowKey));
			ResultScanner results = hTable.getScanner(scan);
			Map<String, Result> map = new HashMap<>();
			for (Result result : results) {
				String key = new String(result.getRow());
				map.put(key, result);
				Get get = new Get(Bytes.toBytes(key));
				get.setMaxVersions(3); // will return last 3 versions of row
				Result rs = hTable.get(get);
			//	System.out.println(tableName + "表RowKey为" + key + "的行数据如下：");
				for (Cell cell : rs.rawCells()) {
					System.out.println("\t查询开始年月份：" + endRowKey);
					System.out.println("\t查询结束年月份：" + startRowKey);
					System.out.println("\t机号为："  + new String(CellUtil.cloneQualifier(cell)));
					System.out.println("\t值序列为：" + new String(CellUtil.cloneValue(cell)));
					System.out.println("\t----------------------------------------------------------");
				}
			}

			list.addAll(map.entrySet());
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (hTable != null) {
				try {
					hTable.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return list;

	}
	
	/***
	 * 建表
	 * @param tableName 表名
	 * @param families	列簇
	 * @throws IOException
	 */
	public static void createTable(String tableName, String[] families) throws IOException {
		init();
		// 初始化
		if (admin.tableExists(TableName.valueOf(tableName)))
		// 如果表已经存在
		{
			System.out.println(tableName + "已存在");
		} else
		// 如果表不存在
		{
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			// 使用表名创建一个表描述器HTableDescriptor对象
			for (String family : families) {
				tableDesc.addFamily(new HColumnDescriptor(family));
				// 添加列族
			}
			admin.createTable(tableDesc);
		}
		Configuration configuration = HBaseConfiguration.create();

		configuration.set("hbase.table.sanity.checks", "false");
		// 更新现有表的split策略
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTable hTable = new HTable(conf, tableName);
		HTableDescriptor htd = hTable.getTableDescriptor();
		HTableDescriptor newHtd = new HTableDescriptor(htd);
		newHtd.setValue("hbase.table.sanity.checks", "false");
		newHtd.setValue(HTableDescriptor.SPLIT_POLICY, "KeyPrefixRegionSplitPolicy");// 指定策略
		newHtd.setValue("prefix_split_key_policy.prefix_length", "6");
		newHtd.setValue("MEMSTORE_FLUSHSIZE", "5242880"); // 5M
		//admin.disableTable(tableName);
		admin.modifyTable(Bytes.toBytes(tableName), newHtd);
		//admin.enableTable(tableName);
		// 创建表
		System.out.println("Table created");
	}

	/**
	 * 新增列簇
	 * @param tableName	表名
	 * @param family	列族名
	 */
	public static void addFamily(String tableName, String family)
	{
		try
		{
			init();
			HColumnDescriptor columnDesc = new HColumnDescriptor(family);
			admin.addColumn(TableName.valueOf(tableName), columnDesc);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			destroy();
		}
	}
 
	/**
	 * 查询表信息
	 * @param conn
	 * @param tableName
	 */
	public static void query(String tableName)
	{
		HTable hTable = null;
		ResultScanner scann = null;
		try
		{
		//	init();
			hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
			scann = hTable.getScanner(new Scan());
			for (Result rs : scann)
			{
				System.out.println("RowKey为：" + new String(rs.getRow()));
				// 按cell进行循环
				for (Cell cell : rs.rawCells())
				{
					System.out.println("列簇为：" + new String(CellUtil.cloneFamily(cell)));
					System.out.println("列修饰符为：" + new String(CellUtil.cloneQualifier(cell)));
					System.out.println("值为：" + new String(CellUtil.cloneValue(cell)));
				}
			/*	RegionLocator r= conn.getRegionLocator(TableName.valueOf(tableName));
		        HRegionLocation location = r.getRegionLocation(Bytes.toBytes( new String(rs.getRow())));
		        HRegionInfo rg = location.getRegionInfo();
		        String regionname = Bytes.toString(rg.getRegionName());
		        String strkey = Bytes.toString(rg.getStartKey());
		        String endkey = Bytes.toString(rg.getEndKey());

		        System.out.println(regionname);
		        System.out.println(strkey);
		        System.out.println(endkey);
				System.out.println("=============================================");*/
			}
			
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (scann != null)
			{
				scann.close();
			}
			if (hTable != null)
			{
				try
				{
					hTable.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		//	destroy();
		}
	}
	
	public static ResultScanner query2(String tableName)
	{
		HTable hTable = null;
		ResultScanner scann = null;
		try
		{
			init();
			hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
			scann = hTable.getScanner(new Scan());
			for (Result rs : scann)
			{
				System.out.println("RowKey为：" + new String(rs.getRow()));
				// 按cell进行循环
				for (Cell cell : rs.rawCells())
				{
					System.out.println("列簇为：" + new String(CellUtil.cloneFamily(cell)));
					System.out.println("列修饰符为：" + new String(CellUtil.cloneQualifier(cell)));
					System.out.println("值为：" + new String(CellUtil.cloneValue(cell)));
				}
				System.out.println("=============================================");
			}
			return scann;
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (scann != null)
			{
				scann.close();
			}
			if (hTable != null)
			{
				try
				{
					hTable.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
			destroy();
		}
		return null;
	}
 
	/**
	 * 根据rowkey查询单行
	 * 
	 * @param conn
	 * @param key
	 * @param tableName
	 */
	public static void queryByRowKey(String key, String tableName)
	{
		HTable hTable = null;
		try
		{
			//init();
			hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(key));
	        get.setMaxVersions(3);  // will return last 3 versions of row
			Result rs = hTable.get(get);
			System.out.println(tableName + "表RowKey为" + key + "的行数据如下：");
			for (Cell cell : rs.rawCells())
			{
				System.out.println("\t列簇为：" + new String(CellUtil.cloneFamily(cell)));
				System.out.println("\t列标识符为：" + new String(CellUtil.cloneQualifier(cell)));
				System.out.println("\t值为：" + new String(CellUtil.cloneValue(cell)));
				System.out.println("\t----------------------------------------------------------");
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (hTable != null)
			{
				try
				{
					hTable.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
			//destroy();
		}
	}
	/**
	    * 批量添加记录到HBase表，同一线程要保证对相同表进行添加操作！
	    *
	    * @param tableName HBase表名
	    * @param rowkey    HBase表的rowkey
	    * @param cf        HBase表的columnfamily
	    * @param column    HBase表的列key
	    * @param value     写入HBase表的值value
	    */
	   public static void bulkput(String tableName, String rowkey, String cf, String column, String value,Integer[] group,Integer total) {
		   HTable hTable = null;
	       try {
	    	   
	           List<Put> list = threadLocal.get();
	           if (list == null) {
	               list = new ArrayList<Put>();
	           }
				Put put = new Put(Bytes.toBytes(rowkey));
				put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
				put.addColumn(Bytes.toBytes("machineData"), Bytes.toBytes("all"), Bytes.toBytes(value));
				list.add(put);
				threadLocal.set(list);
				for (int i = 0; i < group.length; i++) {
					Integer th=group[i];
					if (total.equals(th)) {
						hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
						hTable.put(list);
						list.clear();
						break;
					}
				}
	       } catch (IOException e) {
	           e.printStackTrace();
	       }
	   }
	/**
	 * 插入单行单列簇单列修饰符数据
	 * 
	 * @param conn
	 * @param tableName
	 * @param key
	 * @param family
	 * @param col
	 * @param val
	 */
	public static void addOneRecord(String tableName, String key, String family, String col, String val)
	{
		HTable hTable = null;
		try
		{
			init();
			hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
			Put p = new Put(Bytes.toBytes(key));
			p.addColumn(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(val));
			if (p.isEmpty())
			{
				System.out.println("数据插入异常，请确认数据完整性，稍候重试");
			}
			else
			{
				hTable.put(p);
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (hTable != null)
			{
				try
				{
					hTable.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
			destroy();
		}
	}
 
	/**
	 * 插入单行单列簇多列修饰符数据
	 * 
	 * @param conn
	 * @param tableName
	 * @param key
	 * @param family
	 * @param cols
	 * @param val
	 */
	public static void addMoreRecord(String tableName, String key, String family, Map<String, String> colVal)
	{
		HTable hTable = null;
		try
		{
			hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
			Put p = new Put(Bytes.toBytes(key));
			for (String col : colVal.keySet())
			{
				String val = colVal.get(col);
				if (StringUtils.isNotBlank(val))
				{
					p.addColumn(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(val));
				}
				else
				{
					System.out.println("列值为空，请确认数据完整性");
				}
			}
			// 当put对象没有成功插入数据时，此时调用hTable.put(p)方法会报错：java.lang.IllegalArgumentException:No
			// columns to insert
			if (p.isEmpty())
			{
				System.out.println("数据插入异常，请确认数据完整性，稍候重试");
			}
			else
			{
				hTable.put(p);
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (hTable != null)
			{
				try
				{
					hTable.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
			
		}
	}
	public static void addBatchRecord(String tableName, String key, String family, String[] colVal)
	{
		HTable hTable = null;
		try
		{
			init();
			hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
			Put p = new Put(Bytes.toBytes(key));
			for (String col : colVal)
			{
				p.addColumn(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(col));
				
			}
			// 当put对象没有成功插入数据时，此时调用hTable.put(p)方法会报错：java.lang.IllegalArgumentException:No
			// columns to insert
			if (p.isEmpty())
			{
				System.out.println("数据插入异常，请确认数据完整性，稍候重试");
			}
			else
			{
				hTable.put(p);
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (hTable != null)
			{
				try
				{
					hTable.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
			destroy();
		}
	}
 
	/**
	 * 删除指定名称的列簇
	 * 
	 * @param admin
	 * @param family
	 * @param tableName
	 */
	public static void deleteFamily(String family, String tableName)
	{
		try
		{
			init();
			admin.deleteColumn(TableName.valueOf(tableName), Bytes.toBytes(family));
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			destroy();
		}
	}
 
	/**
	 * 删除指定行
	 * 
	 * @param conn
	 * @param key
	 * @param tableName
	 */
	public static void deleteRow(String key, String tableName)
	{
		HTable hTable = null;
		try
		{
			init();
			hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
			hTable.delete(new Delete(Bytes.toBytes(key)));
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (hTable != null)
			{
				try
				{
					hTable.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
			destroy();
		}
	}
 
	/**
	 * 删除指定表名
	 * 
	 * @param admin
	 * @param tableName
	 */
	public static void deleteTable(String tableName)
	{
		try
		{
			init();
			// 在删除一张表前，必须先使其失效
			admin.disableTable(TableName.valueOf(tableName));
			admin.deleteTable(TableName.valueOf(tableName));
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			destroy();
		}
	}
 
	// 关闭连接
	public static void destroy()
	{
		if (admin != null)
		{
			try
			{
				admin.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		if (conn != null)
		{
			try
			{
				conn.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	@SuppressWarnings("deprecation")
/*	public static void saveClientData(ClientData cd)
	{
		String ymd = cd.getTime().toLocaleString().split(" ")[0];
		Map<String, String> colVal = new HashMap<String, String>();
		colVal.put("s", cd.getScreen());
		colVal.put("m", cd.getModel());
		colVal.put("c", cd.getCountry());
		colVal.put("p", cd.getProvince());
		colVal.put("ci", cd.getCity());
		colVal.put("n", cd.getNetwork());
		colVal.put("t", cd.getTime().toLocaleString());
		addMoreRecord("clientdata_test2",cd.getUserID() + "-" + ymd, "d", colVal);
	}
	*/
	
	
	public static List<Map.Entry> getRangeData(String tableName,long startRowKey,long endRowKey) {
	     HTable hTable = null;
	     List<Map.Entry> list = new ArrayList<>();
		try
		{
			init();
			hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			scan.setTimeRange(startRowKey, endRowKey);
		/*    scan.setStartRow(Bytes.toBytes(startRowKey));
		    scan.setStopRow(Bytes.toBytes(endRowKey)); */
		    ResultScanner results =  hTable.getScanner(scan);
		    Map<String,Result> map = new HashMap<>();
		    for (Result result : results){
		        String key = new String(result.getRow());
		        map.put(key,result);
		        Get get = new Get(Bytes.toBytes(key));
		        get.setMaxVersions(3);  // will return last 3 versions of row
				Result rs = hTable.get(get);
				System.out.println(tableName + "表RowKey为" + key + "的行数据如下：");
				for (Cell cell : rs.rawCells())
				{
					System.out.println("\t列簇为：" + new String(CellUtil.cloneFamily(cell)));
					System.out.println("\t列标识符为：" + new String(CellUtil.cloneQualifier(cell)));
					System.out.println("\t值为：" + new String(CellUtil.cloneValue(cell)));
					System.out.println("\t----------------------------------------------------------");
				}
		    }
		   
		    list.addAll(map.entrySet());
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (hTable != null)
			{
				try
				{
					hTable.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
			destroy();
		}
		return list;

	}

	public static void  updateRawData(String tableName, String startRowKey, String endRowKey) throws ParseException, IOException {
		HTable hTable = null;
		String cf = null;
		String cfsignal = null;
		String line = null;
		Integer sum =null;
		String sendTime = null;
		String keyRow=null;
		List<String> rows=new ArrayList<String>();
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		try {

			hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(startRowKey));
			scan.setStopRow(Bytes.toBytes(endRowKey));
			ResultScanner results = hTable.getScanner(scan);
			Map<String, Result> map = new HashMap<>();
			for (Result result : results) {
				String key = new String(result.getRow());
				map.put(key, result);
				Get get = new Get(Bytes.toBytes(key));
				get.setMaxVersions(3); // will return last 3 versions of row
				Result rs = hTable.get(get);
				for (Cell cell : rs.rawCells()) {
					String temp = null;
					cf = new String(CellUtil.cloneFamily(cell));
					cfsignal = new String(CellUtil.cloneQualifier(cell));
					temp = cfsignal;
					line = new String(CellUtil.cloneValue(cell));
					String[] lines = line.split(",");

					for (String item : lines) {
						if (item.indexOf("机械设备最后通信时间") > -1) {
							lines = item.split(":");
							sendTime = lines[1] + ":" + lines[2] + ":" + lines[3];

							Long timestamp = dateFormat.parse(sendTime).getTime();
							keyRow = String.valueOf(Long.MAX_VALUE - timestamp);
							temp = temp + "@@" + keyRow;
							Integer endIndex = line.lastIndexOf(":") - 16;
							line = line.substring(0, endIndex) + sendTime + "}";
							break;
						}

					}
					temp = temp + "@@" + line;
					rows.add(temp);
				}
			}
			updateHtable(tableName, rows);  
			for (Result result : results) {
				String key = new String(result.getRow());
			    hTable.delete(new Delete(Bytes.toBytes(key)));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (hTable != null) {
				try {
					hTable.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
		

	}
  
	public static void updateHtable(String tableName,  List<String> rows) throws IOException {
		String keyRow = null;
		String machineNo = null;
		Integer sum = rows.size();
		Integer total = 0;
		String line = null;
		try {
			Integer group[] = null;
			Integer n = sum / 100000;
			group = new Integer[n + 1];
			int k = 0;
			for (int i = 1; i <= n; i++) {
				group[k++] = i * 100000;
			}
			group[k] = sum;

			for (String item:  rows) {
				total+=1;
				String items[]=item.split("@@");
				if(items.length==3) {
				machineNo=items[0];
				keyRow = items[1];
				line= items[2];
				
				LGHbaseDataSourceUtil.bulkput(tableName, keyRow, "machineNo", machineNo, line, group, total);
				line= null;
				
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			// 连接的Session和Connection对象都需要关闭

		}

	}
	
   
	public static void queryColumn(String tableName, String columnFamily, String column, String begin, String end) throws IOException {
		HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		List<Filter> filters = new ArrayList<Filter>();
		Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
				CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(begin));
		Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
				CompareFilter.CompareOp.LESS, Bytes.toBytes(end));
		filters.add(filter1);
		filters.add(filter2);
		FilterList filterList = new FilterList(filters);
		scan.setFilter(filterList);
		ResultScanner results = table.getScanner(scan);
		Map<String, Result> map = new HashMap<>();
		for (Result result : results) {
			String key = new String(result.getRow());
			map.put(key, result);

			Get get = new Get(Bytes.toBytes(key));
			get.setMaxVersions(3); // will return last 3 versions of row
			Result rs = table.get(get);
			System.out.println( "起始列为：" + begin +  "   终止列为：" +end);
			for (Cell cell : rs.rawCells()) {
				System.out.println("\t机号为：" + new String(CellUtil.cloneQualifier(cell)));
				System.out.println("\t值序列为：" + new String(CellUtil.cloneValue(cell)));
				System.out.println("\t----------------------------------------------------------");
			}
		}
			
	
		List<Map.Entry> list = new ArrayList<>();
		list.addAll(map.entrySet());

		table.close();

	}
	/*public static void queryByRowKeyRange(String startkey,String endkey, String tableName)
	    HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		List<RowFilter> filters = new ArrayList<RowFilter>();
		RowFilter rowFilter1 = new RowFilter(CompareOp.GREATER_OR_EQUAL, new SubstringComparator("20200701"));
		RowFilter rowFilter2 = new RowFilter(CompareOp.LESS_OR_EQUAL, new SubstringComparator("20200701"));
		filters.add(rowFilter1);
		filters.add(rowFilter2);
		FilterList filterList = new FilterList(filters);
		scan.setFilter(filterList);
		ResultScanner results = table.getScanner(scan);
		Map<String, Result> map = new HashMap<>();
		for (Result result : results) {
			String key = new String(result.getRow());
			map.put(key, result);

			Get get = new Get(Bytes.toBytes(key));
			get.setMaxVersions(3); // will return last 3 versions of row
			Result rs = table.get(get);
			System.out.println( "起始列为：" + begin +  "   终止列为：" +end);
			for (Cell cell : rs.rawCells()) {
				System.out.println("\t机号为：" + new String(CellUtil.cloneQualifier(cell)));
				System.out.println("\t值序列为：" + new String(CellUtil.cloneValue(cell)));
				System.out.println("\t----------------------------------------------------------");
			}
		}
			
	
		List<Map.Entry> list = new ArrayList<>();
		list.addAll(map.entrySet());

		table.close();

	}*/
	
	public static List<Map.Entry> getRangeLgData(String tableName) {
	     HTable hTable = null;
	     List<Map.Entry> list = new ArrayList<>();
		try
		{
		//	init();
			hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
	//		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new SubstringComparator("9223370432"));
		//	rowFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(".*9223370432.*"));		
		//	scan.setFilter(rowFilter);
		    ResultScanner results =  hTable.getScanner(scan);
		    Map<String,Result> map = new HashMap<>();
		    for (Result result : results){
		        String key = new String(result.getRow());
		        map.put(key,result);
		        Get get = new Get(Bytes.toBytes(key));
		        get.setMaxVersions(3);  // will return last 3 versions of row
				Result rs = hTable.get(get);
				System.out.println(tableName + "表RowKey为" + key + "的行数据如下：");
				for (Cell cell : rs.rawCells())
				{
					System.out.println("\t列簇为：" + new String(CellUtil.cloneFamily(cell)));
					System.out.println("\t列标识符为：" + new String(CellUtil.cloneQualifier(cell)));
					System.out.println("\t值为：" + new String(CellUtil.cloneValue(cell)));
					System.out.println("\t----------------------------------------------------------");
				}
		    }
		   
		    list.addAll(map.entrySet());
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (hTable != null)
			{
				try
				{
					hTable.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		//	destroy();
		}
		return list;

	}
	public static List<Map.Entry> getLgRangeData(String tableName) {
	     HTable hTable = null;
	     List<Map.Entry> list = new ArrayList<>();
		try
		{
		//	init();
			hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
		    scan.setStartRow(Bytes.toBytes("0000000020-9223370430677543750"));
		    scan.setStopRow(Bytes.toBytes("0000000020-9223370432294313997"));
		    ResultScanner results =  hTable.getScanner(scan);
		    Map<String,Result> map = new HashMap<>();
		    for (Result result : results){
		        String key = new String(result.getRow());
		        map.put(key,result);
		        Get get = new Get(Bytes.toBytes(key));
		        get.setMaxVersions(3);  // will return last 3 versions of row
				Result rs = hTable.get(get);
				System.out.println(tableName + "表RowKey为" + key + "的行数据如下：");
				for (Cell cell : rs.rawCells())
				{
					System.out.println("\t列簇为：" + new String(CellUtil.cloneFamily(cell)));
					System.out.println("\t列标识符为：" + new String(CellUtil.cloneQualifier(cell)));
					System.out.println("\t值为：" + new String(CellUtil.cloneValue(cell)));
					System.out.println("\t----------------------------------------------------------");
				}
		    }
		   
		    list.addAll(map.entrySet());
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (hTable != null)
			{
				try
				{
					hTable.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		//	destroy();
		}
		return list;

	}

	@SuppressWarnings("unused")
	public static void writeLgHive() throws ParseException, IOException {
		String curDay = null;
		String preDay = null;
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");// 设置日期格式
		String[] args={};//{"20210219","20210220"};
		if (args==null || args.length == 0) {
			curDay = df.format(new Date());// new Date()为获取当前系统时间
			Calendar cld = Calendar.getInstance();
			cld.setTime(new Date());
			cld.add(Calendar.DATE, -1);
			preDay = df.format(cld.getTime());
		} else if (args!=null && args.length == 1) {
			System.err.println("需要输入2个时间参数，格式为：yyyyMMdd");
			System.exit(1);
		}else if (args!=null && (args[0].length() < 8 || args[1].length() < 8)) {
			System.err.println("时间参数长度错误，至少应为：yyyyMMdd");
			System.exit(1);
		}else if (args!=null) {
			curDay = args[1];
			preDay = args[0];
		}
		
		HTable hTable = null;
		HTable originalTable = null;
		String tableName = "mcu_history_index";
		
		try {
			hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
			originalTable = (HTable) conn.getTable(TableName.valueOf("mcu_history"));
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(preDay));
			scan.setStopRow(Bytes.toBytes(curDay));
			ResultScanner results = hTable.getScanner(scan);
			for (Result result : results) {
				String rowKey = new String(result.getRow());
				String dayShortStr = rowKey.split("-")[0];
				String date = dayShortStr.substring(0, 8);
				String hour = dayShortStr.substring(8,10);

				// 读取：d:rowkey
				String originalRowKey = rowKey.substring(rowKey.indexOf("-")+1);
                String machine_id=originalRowKey.split("-")[0];
				Get get = new Get(originalRowKey.getBytes());
				Result originalResult = originalTable.get(get);

				// 生成结果字符串
				StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append(originalRowKey).append("\t");
				stringBuilder.append(machine_id).append("\t");
				stringBuilder.append(date).append("\t");
				//stringBuilder.append(hour).append("\t");
				String qualifier=null;
				String mcu_stream=null;
		        String cellvalue =null;
		        byte[] valueBytes =null;
				Cell[] cells = originalResult.rawCells();
				for (int i = 0; i < cells.length; i++) {
					Cell cell = cells[i];
					byte[] qualifierBytes = Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(),cell.getQualifierLength());
					valueBytes = Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
					
					qualifier=new String(qualifierBytes, StandardCharsets.UTF_8);
					if (qualifier.equals(filterQuality)) {
						
						StringBuilder sb = new StringBuilder(valueBytes.length * 2);
						// 将字节数组中每个字节拆解成2位16进制整数
						for (int j = 0; j < valueBytes.length; j++) {
							sb.append(hexString.charAt((valueBytes[j] & 0xf0) >> 4));
							sb.append(hexString.charAt((valueBytes[j] & 0x0f) >> 0));
						}
						cellvalue =sb.toString();
						mcu_stream=cellvalue;
					}else {
						 cellvalue = new String(valueBytes, StandardCharsets.UTF_8);
					}
					if (!qualifier.equals(filterQuality)) {
					stringBuilder.append(qualifier).append(":").append(cellvalue);
				    }
					if (i < cells.length - 1) {
						stringBuilder.append("$");
					}
				}
				
			    stringBuilder.append("\t");
				stringBuilder.append(mcu_stream).append("\t");
				String type=stringBuilder.toString();
				System.out.println(type);
			//	context.write(NullWritable.get(), new Text(stringBuilder.toString()));
				

			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (hTable != null) {
				try {
					hTable.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static Table indexTable = null;
    private static Table  badIndexTable = null;
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    private static final String [] typeSets= {"F986","F881","F880","F883","F980","FB80","FB81","FB83","02D0"};
    private static final String typeValueSets[] = {"LOWERTOTALFUEL", "TOTALFUELCONSUMPTION", "ENGINEHOURS", "SINGLEFUEL", "LOWERENGINEHOURS", "TOTALFUEL"};
    private static final String hexString = "0123456789ABCDEF";
    private static final String filterQuality="MCU_STREAM_DATA";
      
    public static void writeLgIndexeData() throws ParseException, IOException, DecoderException {
		indexTable = (HTable) conn.getTable(TableName.valueOf("mcu_history_index"));
		HTable histrTable = (HTable) conn.getTable(TableName.valueOf("mcu_history"));
		HTable badIndexTable = (HTable) conn.getTable(TableName.valueOf("mcu_history_bad_index"));
		HashMap<String,ArrayList<String>> filterMap=new HashMap<String,ArrayList<String>>();
		ArrayList<String> al=new ArrayList<String>();
		al.add("TOTALFUEL");
		al.add("ENGINEHOURS");
		filterMap.put("F986", al);
		filterMap.put("F883", al);
		
		al=new ArrayList<String>();
		al.add("TOTALFUEL");
		al.add("ENGINEHOURS");
		al.add("SINGLEFUEL");
		filterMap.put("F881", al);
		filterMap.put("F880", al);
		
		al=new ArrayList<String>();
		al.add("ENGINEHOURS");
		filterMap.put("FB80", al);
		filterMap.put("FB81", al);
		filterMap.put("02D0", al);
		
		al=new ArrayList<String>();
		al.add("ENGINEHOURS");
		al.add("TOTALFUELCONSUMPTION");
		filterMap.put("F980", al);
		
		al=new ArrayList<String>();
		al.add("LOWERENGINEHOURS");
		al.add("LOWERTOTALFUEL");
		filterMap.put("FB83", al);
		String startTime = "20200101000000";
		  String endTime = "20210101000000";
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddhhmmss");
		Boolean badData = false;
		String type="";
		long minStamp = dateFormat.parse(startTime).getTime();
		long maxStamp = dateFormat.parse(endTime).getTime();
		Scan scan = new Scan();
		scan.setTimeRange(minStamp, maxStamp);
		scan.setCaching(10000);
		scan.setCacheBlocks(false);
		ResultScanner results = histrTable.getScanner(scan);
		for (Result result : results) {
			byte[] row = result.getRow();
			String rawKey = new String(row);
			HashMap<String,String> curMap=new HashMap<String,String>();
			for (Cell cell : result.rawCells()) {
				String qualifier = new String(CellUtil.cloneQualifier(cell));
				byte[] valueByte = Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				String cellvalue = new String(valueByte, StandardCharsets.UTF_8);
			
				if (qualifier.equals(filterQuality)) {
					byte[] valueBytes = Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
					StringBuilder sb = new StringBuilder(valueBytes.length * 2);
					// 将字节数组中每个字节拆解成2位16进制整数
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
						type = type.substring(4, 8);
					}
					if (type.startsWith("7E")) {
						type = type.substring(2, 6);
					}
				}
				for (String qf : typeValueSets) {
					if (qf.equals(qualifier)) {
						curMap.put(qualifier, cellvalue);
					}
				}
			 }
			
				for (String tp : typeSets) {
					if (tp.equals(type)) {
						for (String ql : filterMap.get(type)) {
							String filterV = curMap.get(ql);
							if(filterV == null) {
								System.out.println("null");
							}
							if (filterV != null) {
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
						}
					}

				}
			
			String timeKey = rawKey.split("-")[1];
			Long tamstap = Long.MAX_VALUE - Long.valueOf(timeKey);
			String timeIndex = new SimpleDateFormat("yyyyMMddhhmmssSSS").format(tamstap);
			String rowIndex = timeIndex + "-" + rawKey;
			Put putIndex = new Put(Bytes.toBytes(rowIndex));
			putIndex.addColumn(Bytes.toBytes("d"), Bytes.toBytes("k"), Bytes.toBytes(""));
			if (badData==false) {
			//	System.out.println("good"+rawKey);
			//	System.out.println("");
			//	 indexTable.put(putIndex);
			}
			else {
			//	System.out.println("bad"+rawKey);
			//	System.out.println("");
			//	 badIndexTable.put(putIndex);
			}
			badData=false;
           }
		//}
		
	}
  
    public void coprocessorLgIndex() throws ParseException, IOException {
    	HTable histrTable = (HTable) conn.getTable(TableName.valueOf("mcu_history_bad_index"));
    	HTable testTable = (HTable) conn.getTable(TableName.valueOf("mcu_history_shi"));
		String startTime = "20210227000000";
	      String endTime = "20210228000000";
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddhhmmss");

		long minStamp = dateFormat.parse(startTime).getTime();
		long maxStamp = dateFormat.parse(endTime).getTime();
		Scan scan = new Scan();
		scan.setStartRow(startTime.getBytes());
		scan.setStopRow(endTime.getBytes());
		scan.setCaching(10000);
		scan.setCacheBlocks(false);
		ResultScanner results = histrTable.getScanner(scan);
		int count=0;
		for (Result result : results) {
			byte[] row = result.getRow();
			count+=1;
		/*	Put put=new Put(row);
			for (Cell cell : result.rawCells()) {
				
				byte[] qualifier = CellUtil.cloneQualifier(cell);
				byte[] valueByte = Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				put.addColumn("d".getBytes(), qualifier, valueByte);
				
			}
			testTable.put(put);
	    postPut(null,put, null,null) ; */
		}
    	System.out.print(count);
    }  
	
    @SuppressWarnings("unused")
	public  void postPut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put, final WALEdit edit,final Durability durability) throws IOException {
    	HTable indexTable = (HTable) conn.getTable(TableName.valueOf("mcu_history_index"));
		HTable histrTable = (HTable) conn.getTable(TableName.valueOf("mcu_history_shi"));
		HTable badIndexTable = (HTable) conn.getTable(TableName.valueOf("mcu_history_bad_index"));
    	String type="";
    	Boolean badData = false;
    	HashMap<String,ArrayList<String>> filterMap=new HashMap<String,ArrayList<String>>();
		ArrayList<String> al=new ArrayList<String>();
		al.add("TOTALFUEL");
		al.add("ENGINEHOURS");
		filterMap.put("F986", al);
		filterMap.put("F883", al);
		
		al=new ArrayList<String>();
		al.add("TOTALFUEL");
		al.add("ENGINEHOURS");
		al.add("SINGLEFUEL");
		filterMap.put("F881", al);
		filterMap.put("F880", al);
		
		al=new ArrayList<String>();
		al.add("ENGINEHOURS");
		filterMap.put("FB80", al);
		filterMap.put("FB81", al);
		filterMap.put("02D0", al);
		
		al=new ArrayList<String>();
		al.add("ENGINEHOURS");
		al.add("TOTALFUELCONSUMPTION");
		filterMap.put("F980", al);
		
		al=new ArrayList<String>();
		al.add("LOWERENGINEHOURS");
		al.add("LOWERTOTALFUEL");
		filterMap.put("FB83", al);
		String rawKey = new String(put.getRow());
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
			indexTable.put(putIndex);
		} else {
			badIndexTable.put(putIndex);
		}
		badData = false;

	}
	
	
	

	public static void main(String[] args) throws Exception {
		init();
		
		new LGHbaseDataSourceUtil().coprocessorLgIndex();
		
		
	}
}
