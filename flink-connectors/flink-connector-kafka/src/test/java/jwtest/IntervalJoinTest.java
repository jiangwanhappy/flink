package jwtest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author jiangshine
 * @version 1.0
 * @date 2021/6/22 16:51
 * @Content
 */
public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment dbEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        dbEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        dbEnv.setParallelism(1);
        //设置水印生成时间间隔6m,方便测试
//        dbEnv.getConfig().setAutoWatermarkInterval(3 * 60 * 1000);//默认200

        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment dbTableEnv = StreamTableEnvironment.create(dbEnv, bsSettings);
        // access flink configuration
        Configuration configuration = dbTableEnv.getConfig().getConfiguration();
        // set low-level key-value options
        //检测流是否空闲的时间
//        configuration.setString("table.exec.source.idle-timeout", "120000 ms");//5 * 60 * 1000 = 300000

        String source_001 = "CREATE TABLE `transflowTable`("
                + "tran_no STRING,"
                + "cust_no STRING,"
                + "acct_no STRING,"
                + "amount DOUBLE,"
                + "status STRING,"
                + "trade_time TIMESTAMP(3),"
                + "WATERMARK FOR trade_time as trade_time - INTERVAL '5' SECOND"
                + ") WITH ("
//                + "'connector' = 'kafka-0.11',"
                + "'connector' = 'kafka',"
                + "'topic' = 'transflow',"
                + "'properties.bootstrap.servers' = 'master:9092',"
                + "'properties.group.id' = 'test00001',"
                + "'scan.startup.mode' = 'latest-offset',"
                + "'format' = 'json')";

        String source_002 = "CREATE TABLE custTable("
                + "cust_no STRING,"
                + "cert_no STRING,"
                + "cust_name STRING,"
                + "addr STRING,"
//                + "iphone STRING,"
                + "create_time TIMESTAMP(3),"
                + "WATERMARK FOR create_time as create_time - INTERVAL '4' SECOND"
                + ") WITH ("
//                + "'connector' = 'kafka-0.11' ,"
                + "'connector' = 'kafka',"
                + "'topic' = 'custTable',"
                + "'properties.bootstrap.servers' = 'master:9092',"
                + "'properties.group.id' = 'test00001',"
                + "'scan.startup.mode' = 'latest-offset',"
                + "'format' = 'json')";
        dbTableEnv.executeSql(source_001);
        dbTableEnv.executeSql(source_002);
//        HashMap<String, String> stringStringHashMap = new HashMap<>();
//        stringStringHashMap.put("idleTimeout", 5 * 60 * 1000 + "");//5分钟
//        stringStringHashMap.put("two_stream_join_delay_time", (2000 + ((2000 + 3000) / 2)  + 1) + "");
//        ParameterTool parameterTool = ParameterTool.fromMap(stringStringHashMap);
//        dbEnv.getConfig().setGlobalJobParameters(parameterTool);


        String sinkSql = "CREATE TABLE twojoin_sink_table (" +
                "tran_no STRING,cust_no STRING,cust_name STRING" +
                ") WITH ('connector' = 'kafka','topic' = 'twojoin_sink_table','properties.bootstrap.servers' = 'master:9092','format' = 'json','sink.partitioner' = 'round-robin')";
        dbTableEnv.executeSql(sinkSql);

//        String join_sql = "SELECT transflowTable.tran_no, transflowTable.cust_no, transflowTable.trade_time, custTable.cust_name"
        String join_sql = "create view rule2_view as SELECT transflowTable.tran_no as tran_no, transflowTable.cust_no as cust_no,custTable.cust_name as cust_name"
                + " FROM transflowTable "
                + " LEFT JOIN custTable "
                + "ON transflowTable.cust_no = custTable.cust_no " //是作为双流的key
                + "AND transflowTable.trade_time BETWEEN custTable.create_time - INTERVAL '2' SECOND AND custTable.create_time + INTERVAL '3' SECOND"
                ;


//        String jiSunSql = "SELECT product_no, count(trade_amount)  over(PARTITION BY product_no ORDER BY order_time RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW)  AS trade_amount_sum_per_type_001_003  FROM interval_join_001_interval_join_002";

//        TableResult table = dbTableEnv.sqlQuery(join_sql).execute();
//        table.print();
//        dbTableEnv.executeSql(join_sql).print();
//        dbTableEnv.executeSql(join_sql);
        TableResult table = dbTableEnv.executeSql(join_sql);

//        Future callback
//        table.getJobClient().get().getJobStatus().thenApply({
//
//        });
//        CompletableFuture.completedFuture

        String insertSql = "insert into twojoin_sink_table select tran_no,cust_no,cust_name from rule2_view";
//        System.out.println("rule2Sql:" + insertSql);
        dbTableEnv.executeSql(insertSql);

    }
}
