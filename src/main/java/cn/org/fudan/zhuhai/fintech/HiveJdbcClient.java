package cn.org.fudan.zhuhai.fintech;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
 
public class HiveJdbcClient {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	@SuppressWarnings("unused")
	public void run() {

		try {
			Class.forName(driverName);
			// 配置结点在DN1,端口配置为10000，数据库是默认的default，数据库用户名为//hive，密码为·hive,如需更换在hive-site.xml中配置。详见配置细节
			Connection con = DriverManager.getConnection("jdbc:hive2://DN1:10000/default", "hive", "hive");
			Statement stmt = con.createStatement();
			// 查询某天的数据
			String sql = "select * from mcu_history where partition_day=20210227   and machine_id=0000003016  ";
			ResultSet res = stmt.executeQuery(sql);
			while (res.next()) {
				String rowkey = res.getString("rowkey");
				String machine_id = res.getString("machine_id");
				String key_value_set = res.getString("key_value_set");
				String mcu_stream = res.getString("mcu_stream");
				String mcu_history_day = res.getString("partition_day");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws SQLException {
		HiveJdbcClient hiveJdbcClient = new HiveJdbcClient();
		hiveJdbcClient.run();
	}
}
