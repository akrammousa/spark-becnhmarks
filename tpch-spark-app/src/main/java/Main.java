import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.io.*;
import java.util.ArrayList;
import static org.apache.spark.sql.types.DataTypes.*;

public class Main {

    private static void  writeDFInFile(Dataset<Row> df, String logsPath){
        try {
            FileWriter logical_plan_file = new FileWriter(logsPath + "logical.txt");

            FileWriter physical_plan_file = new FileWriter("/mnt/01D43FA6387D16F0/GP-general/SparkConfigurationsAutotuning/resources/physical.txt");

            logical_plan_file.write(df.queryExecution().optimizedPlan().toString());

            physical_plan_file.write(df.queryExecution().executedPlan().toString());

            // Closing printwriter
            logical_plan_file.close();

            physical_plan_file.close();
            System.out.println("plans wrote successfully");
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
    private static void runBenchmarkQuery(String query, String message, SparkSession spark, ArrayList<Long> runTimes , String logsPath){
        System.out.println("Starting: " + message);
        //start time
        long queryStartTime = System.currentTimeMillis();
        //run the query and show the result
        Dataset<Row> df = spark.sql(query);
        df.show(5);
        writeDFInFile(df , logsPath);
        //end time
        long queryStopTime = System.currentTimeMillis();
        long runTime = (long) ((queryStopTime-queryStartTime) / 1000F);
        runTimes.add(runTime);
        System.out.println("Runtime:" + runTime + "seconds");
        System.out.println("Finishing: " + message);
    }
    private static void writeTheParquetData(SparkSession spark, String tblReadPath,String writeParquetPath){
        StructType customer_schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("c_custkey", LongType, true),
                DataTypes.createStructField("c_name", StringType, true),
                DataTypes.createStructField("c_address", StringType, true),
                DataTypes.createStructField("c_nationkey", LongType, true),
                DataTypes.createStructField("c_phone", StringType, true),
                DataTypes.createStructField("c_acctbal", DoubleType, true),
                DataTypes.createStructField("c_mktsegment", StringType, true),
                DataTypes.createStructField("c_comment", StringType, true)
        });
        StructType region_schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("r_regionkey", IntegerType, true),
                DataTypes.createStructField("r_name", StringType, true),
                DataTypes.createStructField("r_comment", StringType, true),
        });
        StructType nation_schema = DataTypes.createStructType(new StructField[]{
                createStructField("n_nationkey", IntegerType,true),
                createStructField("n_name", StringType,true),
                createStructField("n_regionkey", IntegerType,true),
                createStructField("n_comment", StringType,true)

        });
        StructType part_schema = DataTypes.createStructType(new StructField[]{
                createStructField("p_partkey", IntegerType,true),
                createStructField("p_name", StringType,true),
                createStructField("p_mfgr", StringType,true),
                createStructField("p_brand", StringType,true),
                createStructField("p_type", StringType,true),
                createStructField("p_size", IntegerType,true),
                createStructField("p_container", StringType,true),
                createStructField("p_retailprice", DoubleType,true),
                createStructField("p_comment", StringType,true)
        });
        StructType supplier_schema = DataTypes.createStructType(new StructField[]{
                createStructField("s_suppkey", IntegerType,true),
                createStructField("s_name", StringType,true),
                createStructField("s_address", StringType,true),
                createStructField("s_nationkey", IntegerType,true),
                createStructField("s_phone", StringType,true),
                createStructField("s_acctbal", DoubleType,true),
                createStructField("s_comment", StringType,true)
        });
        StructType part_supplier_schema = DataTypes.createStructType(new StructField[]{
                createStructField("ps_partkey", IntegerType,true),
                createStructField("ps_suppkey", IntegerType,true),
                createStructField("ps_availqty", IntegerType,true),
                createStructField("ps_supplycost", DoubleType,true),
                createStructField("ps_comment", StringType,true)
        });
        StructType order_schema = DataTypes.createStructType(new StructField[]{
                createStructField("o_orderkey", IntegerType,true),
                createStructField("o_custkey", IntegerType,true),
                createStructField("o_orderstatus", StringType,true),
                createStructField("o_totalprice", DoubleType,true),
                createStructField("o_orderdate", StringType,true),
                createStructField("o_orderpriority", StringType,true),
                createStructField("o_clerk", StringType,true),
                createStructField("o_shippriority", IntegerType,true),
                createStructField("o_comment", StringType,true)
        });
        StructType lint_item_schema = DataTypes.createStructType(new StructField[]{
                createStructField("l_orderkey", IntegerType,true),
                createStructField("l_partkey", IntegerType,true),
                createStructField("l_suppkey", IntegerType,true),
                createStructField("l_linenumber", IntegerType,true),
                createStructField("l_quantity", DoubleType,true),
                createStructField("l_extendedprice", DoubleType,true),
                createStructField("l_discount", DoubleType,true),
                createStructField("l_tax", DoubleType,true),
                createStructField("l_returnflag", StringType,true),
                createStructField("l_linestatus", StringType,true),
                createStructField("l_shipdate", StringType,true),
                createStructField("l_commitdate", StringType,true),
                createStructField("l_receiptdate", StringType,true),
                createStructField("l_shipinstruct", StringType,true),
                createStructField("l_shipmode", StringType,true),
                createStructField("l_comment", StringType,true)
        });

        //write customer schema into parquet
        spark.read().option("header","false").format("csv").schema(customer_schema).option("delimiter", "|").csv(tblReadPath + "customer.tbl").write().mode("overwrite").parquet(writeParquetPath+"customer");
        //done
        ///write supplier schema into parquet
        spark.read().format("csv").schema(nation_schema).option("delimiter", "|").load(tblReadPath +"nation.tbl").write().mode("overwrite").parquet(writeParquetPath+"nation");
        //done
        //write supplier schema into parquet
        spark.read().format("csv").schema(lint_item_schema).option("delimiter", "|").load(tblReadPath +"lineitem.tbl").write().mode("overwrite").parquet(writeParquetPath+"lineitem");
        //done
        //write supplier schema into parquet
        spark.read().format("csv").schema(supplier_schema).option("delimiter", "|").load(tblReadPath +"supplier.tbl").write().mode("overwrite").parquet(writeParquetPath+"supplier");
        //done
        //write supplier schema into parquet
        spark.read().format("csv").schema(region_schema).option("delimiter", "|").load(tblReadPath +"region.tbl").write().mode("overwrite").parquet(writeParquetPath+"region");
        //done
        //write supplier schema into parquet
        spark.read().format("csv").schema(part_supplier_schema).option("delimiter", "|").load(tblReadPath +"partsupp.tbl").write().mode("overwrite").parquet(writeParquetPath+"partsupp");
        //done
        //write supplier schema into parquet
        spark.read().format("csv").schema(part_schema).option("delimiter", "|").load(tblReadPath +"part.tbl").write().mode("overwrite").parquet(writeParquetPath+"part");
        //done
        //write supplier schema into parquet
        spark.read().format("csv").schema(order_schema).option("delimiter", "|").load(tblReadPath +"orders.tbl").write().mode("overwrite").parquet(writeParquetPath+"orders");
        //done
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Java Spark");
        SparkSession spark = SparkSession
                .builder()
                .getOrCreate();


        if (args.length < 5){
            return;
        }
        String tblReadPath = args[0];
        String ParquetPath = args[1];
        String sqlQueriesPath = args[2];
        String queriesNumbers = args[3];
        String logsPath = args[4];
        if(tblReadPath.compareToIgnoreCase("FALSE") != 0) {
            writeTheParquetData(spark, tblReadPath, ParquetPath);
            return;
        }

        //runBenchmarkQuery("Select * From CUSTOMER", "Run 1",spark);
        Dataset<Row> dataset = spark.read().parquet(ParquetPath + "customer");
        dataset.registerTempTable("CUSTOMER");

        dataset = spark.read().parquet(ParquetPath +"lineitem");
        dataset.registerTempTable("LINEITEM");

        dataset = spark.read().parquet(ParquetPath +"nation");
        dataset.registerTempTable("NATION");

        dataset = spark.read().parquet(ParquetPath +"orders");
        dataset.registerTempTable("ORDERS");

        dataset = spark.read().parquet(ParquetPath +"part");
        dataset.registerTempTable("PART");

        dataset = spark.read().parquet(ParquetPath +"partsupp");
        dataset.registerTempTable("PARTSUPP");

        dataset = spark.read().parquet(ParquetPath +"region");
        dataset.registerTempTable("REGION");

        dataset = spark.read().parquet(ParquetPath +"supplier");
        dataset.registerTempTable("SUPPLIER");


        ArrayList<Long> runTimes = new ArrayList<Long>();
        String[] queriesNumbersArray = queriesNumbers.split(",");
        for (int i = 0; i < queriesNumbersArray.length; i++) {
            StringBuilder sb = new StringBuilder();
            String line = null;
            BufferedReader bufferedReader = null;
            try {
                bufferedReader = new BufferedReader(
                        new FileReader(sqlQueriesPath + Integer.parseInt(queriesNumbersArray[i]) + ".sql")
                );
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            while (true)
            {
                try {
                    if (!((line = bufferedReader.readLine()) != null)) break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                sb.append(line + " ");
            }
            String[] queries = sb.toString().split(";");
            for (int j = 0; j <queries.length-1 ; j++) {
                runBenchmarkQuery(queries[j],"RUN query number "+queriesNumbersArray[i],spark,runTimes ,logsPath);
            }
        }

        //end time
        long totalRunTime = 0;
        for (int i = 0; i < runTimes.size(); i++) {
            totalRunTime += runTimes.get(i);
        }
        System.out.println("Runtime Of all Queries:" + totalRunTime + "seconds");

    }

}

