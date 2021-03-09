package cn.org.fudan.zhuhai.fintech;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.hadoop.hbase.filter.*; 

@SuppressWarnings("unused")
public class MRDebugDemo
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
	//	conf.set("hbase.zookeeper.quorum", "192.168.3.121:2181,192.168.3.123:2181,192.168.3.124:2181"); //珠海
		conf.set("hbase.zookeeper.quorum", "172.17.27.116:2181,72.17.27.117:2181,172.17.27.118:2181"); //测试
  	//  conf.set("hbase.zookeeper.quorum", "172.17.27.113:2181,72.17.27.107:2181,172.17.27.110:2181"); 	//生产
		//设置zookeeper的结点主机名
		conn = ConnectionFactory.createConnection(conf);
		//使用配置创建一个HBase连接
		admin = conn.getAdmin();
		//得到管理器
	}

	//模拟 HBase2HiveMapper MR debug
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
	
	
	//模拟BuildHBaseIndexMapper MR debug
	private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    private static final String [] typeSets= {"F986","F881","F880","F883","F980","FB80","FB81","FB83","02D0"};
    private static final String typeValueSets[] = {"LOWERTOTALFUEL", "TOTALFUELCONSUMPTION", "ENGINEHOURS", "SINGLEFUEL", "LOWERENGINEHOURS", "TOTALFUEL"};
    private static final String hexString = "0123456789ABCDEF";
    private static final String filterQuality="MCU_STREAM_DATA";
      
    public static void writeLgIndexeData() throws ParseException, IOException, DecoderException {
    	HTable goodTable = (HTable) conn.getTable(TableName.valueOf("mcu_history_index"));
		HTable histrTable = (HTable) conn.getTable(TableName.valueOf("mcu_history"));
		HTable badTable = (HTable) conn.getTable(TableName.valueOf("mcu_history_bad_index"));
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
				 goodTable.put(putIndex);
			}
			else {
			//	System.out.println("bad"+rawKey);
			//	System.out.println("");
				 badTable.put(putIndex);
			}
			badData=false;
           }
		//}
		
	}
    
    //模拟协处理器 MR debug
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
			Put put=new Put(row);
			for (Cell cell : result.rawCells()) {
				
				byte[] qualifier = CellUtil.cloneQualifier(cell);
				byte[] valueByte = Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				put.addColumn("d".getBytes(), qualifier, valueByte);
				
			}
			testTable.put(put);
	    postPut(null,put, null,null) ; 
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
		
		new MRDebugDemo().coprocessorLgIndex();
		
		
	}
}
