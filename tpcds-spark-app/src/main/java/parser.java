import java.io.*;

public class parser {
    public static void main(String[] args) {
        File file = new File("/mnt/01D43FA6387D16F0/GP-general/tpcds-spark-app/src/main/java/test");

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));
            String st;
            while ((st = br.readLine()) != null) {
                String name = st;
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("dataset = spark.read().parquet(ParquetPath +\"").append(name).append("\");\n" + "dataset.registerTempTable(\"").append(name.toUpperCase()).append("\");");
                System.out.println(stringBuilder.toString());
//                dataset = spark.read().parquet(ParquetPath +"lineitem");
//                dataset.registerTempTable("LINEITEM");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}

