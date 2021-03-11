package cn.org.fudan.zhuhai.fintech;

import com.alibaba.druid.pool.DruidDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

public class HiveDataSourceUtil {
    private static DruidDataSource hiveDataSource = new DruidDataSource();
    public static Connection conn = null;
   
   //连接池配置文档
    private static String hive_jdbc_url="jdbc:hive2://DN1:10000/default";
    private static String hive_jdbc_username="hive";
    private static String hive_jdbc_password="hive";

    //配置初始化大小、最小、最大
    private static Integer hive_initialSize=20;
    private static Integer hive_minIdle=20;
    private static Integer  hive_maxActive=500;

    //配置获取连接等待超时的时间
    private static Integer hive_maxWait=60000;
    
    public static DruidDataSource getHiveDataSource() {
        if(hiveDataSource.isInited()){  
            return hiveDataSource;
        }         
                   
        try {       
            hiveDataSource.setUrl(hive_jdbc_url);
            hiveDataSource.setUsername(hive_jdbc_username);
            hiveDataSource.setPassword(hive_jdbc_password);

            //配置初始化大小、最小、最大
            hiveDataSource.setInitialSize(hive_initialSize);
            hiveDataSource.setMinIdle(hive_minIdle);
            hiveDataSource.setMaxActive(hive_maxActive);

            //配置获取连接等待超时的时间
            hiveDataSource.setMaxWait(hive_maxWait);

            //配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
            hiveDataSource.setTimeBetweenEvictionRunsMillis(60000);

            //配置一个连接在池中最小生存的时间，单位是毫秒
            hiveDataSource.setMinEvictableIdleTimeMillis(300000);


            hiveDataSource.setTestWhileIdle(false);

            //打开PSCache，并且指定每个连接上PSCache的大小
            hiveDataSource.setPoolPreparedStatements(true);
            hiveDataSource.setMaxPoolPreparedStatementPerConnectionSize(20);
   
            //配置监控统计拦截的filters
//            hiveDataSource.setFilters("stat");

            hiveDataSource.init();
        } catch (SQLException e) {
            e.printStackTrace();
            closeHiveDataSource();
        }
        return hiveDataSource;
    }

    /**
     *@Description:关闭Hive连接池
     */
    public static void closeHiveDataSource(){
        if(hiveDataSource != null){
            hiveDataSource.close();
        }
    }  

    /**
     * 
     *@Description:获取Hive连接 
     *@return
     */
    public static Connection getHiveConn(){
        try {
            hiveDataSource = getHiveDataSource();
            conn = hiveDataSource.getConnection();
        } catch (SQLException e) {
           
        }
        return conn;
    }

    /**
     *@Description:关闭Hive数据连接
     */
    public static void closeConn(){
        try {
            if(conn != null){
                conn.close();
            }
        } catch (SQLException e) {
          
        }
    }
   

    public static void main(String[] args) throws Exception {
        DataSource ds = HiveDataSourceUtil.getHiveDataSource();
        Connection conn = ds.getConnection();
        Statement stmt = null;
       
            stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery("select * from mcu_history where partition_day=20210227   and machine_id=0000003016 ");  
             while(res.next()){
              System.out.println(res.getString(1));
               
            }
       

        stmt.close();  
        conn.close();
    }
}
