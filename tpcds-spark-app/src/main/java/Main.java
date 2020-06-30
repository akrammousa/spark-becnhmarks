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

    private static void runBenchmarkQuery(String query, String message, SparkSession spark, ArrayList<Long> runTimes, String logsPath){
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
        StructType call_center = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("cc_call_center_sk", IntegerType, true),
                DataTypes.createStructField("cc_call_center_id", StringType, true),
                DataTypes.createStructField("cc_rec_start_date", StringType, true),
                DataTypes.createStructField("cc_rec_end_date", StringType, true),
                DataTypes.createStructField("cc_closed_date_sk", IntegerType, true),
                DataTypes.createStructField("cc_open_date_sk", IntegerType, true),
                DataTypes.createStructField("cc_name", StringType, true),
                DataTypes.createStructField("cc_class", StringType, true),
                createStructField("cc_employees", IntegerType,true),
                createStructField("cc_sq_ft", IntegerType,true),
                createStructField("cc_hours", StringType,true),
                createStructField("cc_manager", StringType,true),
                createStructField("cc_mkt_id", IntegerType,true),
                createStructField("cc_mkt_class", StringType,true),
                createStructField("cc_mkt_desc", StringType,true),
                createStructField("cc_market_manager", StringType,true),
                createStructField("cc_division", IntegerType,true),
                createStructField("cc_division_name", StringType,true),
                createStructField("cc_company", IntegerType,true),
                createStructField("cc_company_name", StringType,true),
                createStructField("cc_street_number", StringType,true),
                createStructField("cc_street_name", StringType,true),
                createStructField("cc_street_type", StringType,true),
                createStructField("cc_suite_number", StringType,true),
                createStructField("cc_city", StringType,true),
                createStructField("cc_county", StringType,true),
                createStructField("cc_state", StringType,true),
                createStructField("cc_zip", StringType,true),
                createStructField("cc_country", StringType,true),
                createStructField("cc_gmt_offset", DoubleType,true),
                createStructField("cc_tax_percentage", DoubleType,true)
        });
        StructType catalog_page = DataTypes.createStructType(new StructField[]{
                createStructField("cp_catalog_page_sk", IntegerType,true),
                createStructField("cp_catalog_page_id", StringType,true),
                createStructField("cp_start_date_sk", IntegerType,true),
                createStructField("cp_end_date_sk", IntegerType,true),
                createStructField("cp_department", StringType,true),
                createStructField("cp_catalog_number", IntegerType,true),
                createStructField("cp_catalog_page_number", IntegerType,true),
                createStructField("cp_description", StringType,true),
                createStructField("cp_type", StringType,true)
        });
        StructType catalog_returns = DataTypes.createStructType(new StructField[]{
                createStructField("cr_returned_date_sk", IntegerType,true),
                createStructField("cr_returned_time_sk", IntegerType,true),
                createStructField("cr_item_sk", IntegerType,true),
                createStructField("cr_refunded_customer_sk", IntegerType,true),
                createStructField("cr_refunded_cdemo_sk", IntegerType,true),
                createStructField("cr_refunded_hdemo_sk", IntegerType,true),
                createStructField("cr_refunded_addr_sk", IntegerType,true),
                createStructField("cr_returning_customer_sk", IntegerType,true),
                createStructField("cr_returning_cdemo_sk", IntegerType,true),
                createStructField("cr_returning_hdemo_sk", IntegerType,true),
                createStructField("cr_returning_addr_sk", IntegerType,true),
                createStructField("cr_call_center_sk", IntegerType,true),
                createStructField("cr_catalog_page_sk", IntegerType,true),
                createStructField("cr_ship_mode_sk", IntegerType,true),
                createStructField("cr_warehouse_sk", IntegerType,true),
                createStructField("cr_reason_sk", IntegerType,true),
                createStructField("cr_order_number", IntegerType,true),
                createStructField("cr_return_quantity", IntegerType,true),
                createStructField("cr_return_amount", DoubleType,true),
                createStructField("cr_return_tax", DoubleType,true),
                createStructField("cr_return_amt_inc_tax", DoubleType,true),
                createStructField("cr_fee", DoubleType,true),
                createStructField("cr_return_ship_cost", DoubleType,true),
                createStructField("cr_refunded_cash", DoubleType,true),
                createStructField("cr_reversed_charge", DoubleType,true),
                createStructField("cr_store_credit", DoubleType,true),
                createStructField("cr_net_loss", DoubleType,true)

        });
        StructType catalog_sales = DataTypes.createStructType(new StructField[]{
                createStructField("cs_sold_date_sk", IntegerType,true),
                createStructField("cs_sold_time_sk", IntegerType,true),
                createStructField("cs_ship_date_sk", IntegerType,true),
                createStructField("cs_bill_customer_sk", IntegerType,true),
                createStructField("cs_bill_cdemo_sk", IntegerType,true),
                createStructField("cs_bill_hdemo_sk", IntegerType,true),
                createStructField("cs_bill_addr_sk", IntegerType,true),
                createStructField("cs_ship_customer_sk", IntegerType,true),
                createStructField("cs_ship_cdemo_sk", IntegerType,true),
                createStructField("cs_ship_hdemo_sk", IntegerType,true),
                createStructField("cs_ship_addr_sk", IntegerType,true),
                createStructField("cs_call_center_sk", IntegerType,true),
                createStructField("cs_catalog_page_sk", IntegerType,true),
                createStructField("cs_ship_mode_sk", IntegerType,true),
                createStructField("cs_warehouse_sk", IntegerType,true),
                createStructField("cs_item_sk", IntegerType,true),
                createStructField("cs_promo_sk", IntegerType,true),
                createStructField("cs_order_number", IntegerType,true),
                createStructField("cs_quantity", IntegerType,true),
                createStructField("cs_wholesale_cost", DoubleType,true),
                createStructField("cs_list_price", DoubleType,true),
                createStructField("cs_sales_price", DoubleType,true),
                createStructField("cs_ext_discount_amt", DoubleType,true),
                createStructField("cs_ext_sales_price", DoubleType,true),
                createStructField("cs_ext_wholesale_cost", DoubleType,true),
                createStructField("cs_ext_list_price", DoubleType,true),
                createStructField("cs_ext_tax", DoubleType,true),
                createStructField("cs_coupon_amt", DoubleType,true),
                createStructField("cs_ext_ship_cost", DoubleType,true),
                createStructField("cs_net_paid", DoubleType,true),
                createStructField("cs_net_paid_inc_tax", DoubleType,true),
                createStructField("cs_net_paid_inc_ship", DoubleType,true),
                createStructField("cs_net_paid_inc_ship_tax", DoubleType,true),
                createStructField("cs_net_profit", DoubleType,true)
        });
        StructType customer = DataTypes.createStructType(new StructField[]{
                createStructField("c_customer_sk", IntegerType,true),
                createStructField("c_customer_id", StringType,true),
                createStructField("c_current_cdemo_sk", IntegerType,true),
                createStructField("c_current_hdemo_sk", IntegerType,true),
                createStructField("c_current_addr_sk", IntegerType,true),
                createStructField("c_first_shipto_date_sk", IntegerType,true),
                createStructField("c_first_sales_date_sk", IntegerType,true),
                createStructField("c_salutation", StringType,true),
                createStructField("c_first_name", StringType,true),
                createStructField("c_last_name", StringType,true),
                createStructField("c_preferred_cust_flag", StringType,true),
                createStructField("c_birth_day", IntegerType,true),
                createStructField("c_birth_month", IntegerType,true),
                createStructField("c_birth_year", IntegerType,true),
                createStructField("c_birth_country", StringType,true),
                createStructField("c_login", StringType,true),
                createStructField("c_email_address", StringType,true),
                createStructField("c_last_review_date", StringType,true)
        });
        StructType customer_address = DataTypes.createStructType(new StructField[]{
                createStructField("ca_address_sk", IntegerType,true),
                createStructField("ca_address_id", StringType,true),
                createStructField("ca_street_number", StringType,true),
                createStructField("ca_street_name", StringType,true),
                createStructField("ca_street_type", StringType,true),
                createStructField("ca_suite_number", StringType,true),
                createStructField("ca_city", StringType,true),
                createStructField("ca_county", StringType,true),
                createStructField("ca_state", StringType,true),
                createStructField("ca_zip", StringType,true),
                createStructField("ca_country", StringType,true),
                createStructField("ca_gmt_offset", DoubleType,true),
                createStructField("ca_location_type", StringType,true)
        });
        StructType customer_demographics = DataTypes.createStructType(new StructField[]{
                createStructField("cd_demo_sk", IntegerType,true),
                createStructField("cd_gender", StringType,true),
                createStructField("cd_marital_status", StringType,true),
                createStructField("cd_education_status", StringType,true),
                createStructField("cd_purchase_estimate", IntegerType,true),
                createStructField("cd_credit_rating", StringType,true),
                createStructField("cd_dep_count", IntegerType,true),
                createStructField("cd_dep_employed_count", IntegerType,true),
                createStructField("cd_dep_college_count", IntegerType,true)
        });
        StructType date_dim = DataTypes.createStructType(new StructField[]{
                createStructField("d_date_sk", IntegerType,true),
                createStructField("d_date_id", StringType,true),
                createStructField("d_date", StringType,true),
                createStructField("d_month_seq", IntegerType,true),
                createStructField("d_week_seq", IntegerType,true),
                createStructField("d_quarter_seq", IntegerType,true),
                createStructField("d_year", IntegerType,true),
                createStructField("d_dow", IntegerType,true),
                createStructField("d_moy", IntegerType,true),
                createStructField("d_dom", IntegerType,true),
                createStructField("d_qoy", IntegerType,true),
                createStructField("d_fy_year", IntegerType,true),
                createStructField("d_fy_quarter_seq", IntegerType,true),
                createStructField("d_fy_week_seq", IntegerType,true),
                createStructField("d_day_name", StringType,true),
                createStructField("d_quarter_name", StringType,true),
                createStructField("d_holiday", StringType,true),
                createStructField("d_weekend", StringType,true),
                createStructField("d_following_holiday", StringType,true),
                createStructField("d_first_dom", IntegerType,true),
                createStructField("d_last_dom", IntegerType,true),
                createStructField("d_same_day_ly", IntegerType,true),
                createStructField("d_same_day_lq", IntegerType,true),
                createStructField("d_current_day", StringType,true),
                createStructField("d_current_week", StringType,true),
                createStructField("d_current_month", StringType,true),
                createStructField("d_current_quarter", StringType,true),
                createStructField("d_current_year", StringType,true)
        });
        StructType household_demographics = DataTypes.createStructType(new StructField[]{
                createStructField("hd_demo_sk", IntegerType,true),
                createStructField("hd_income_band_sk", IntegerType,true),
                createStructField("hd_buy_potential", StringType,true),
                createStructField("hd_dep_count", IntegerType,true),
                createStructField("hd_vehicle_count", IntegerType,true),

        });
        StructType income_band = DataTypes.createStructType(new StructField[]{
                createStructField("ib_income_band_sk", IntegerType,true),
                createStructField("ib_lower_bound", IntegerType,true),
                createStructField("ib_upper_bound", IntegerType,true),
        });
        StructType inventory = DataTypes.createStructType(new StructField[]{
                createStructField("inv_date_sk", IntegerType,true),
                createStructField("inv_item_sk", IntegerType,true),
                createStructField("inv_warehouse_sk", IntegerType,true),
                createStructField("inv_quantity_on_hand", LongType,true),
        });

        StructType item = DataTypes.createStructType(new StructField[]{
                createStructField("i_item_sk", IntegerType,true),
                createStructField("i_item_id", StringType,true),
                createStructField("i_rec_start_date", StringType,true),
                createStructField("i_rec_end_date", StringType,true),
                createStructField("i_item_desc", StringType,true),
                createStructField("i_current_price", DoubleType,true),
                createStructField("i_wholesale_cost", DoubleType,true),
                createStructField("i_brand_id", IntegerType,true),
                createStructField("i_brand", StringType,true),
                createStructField("i_class_id", IntegerType,true),
                createStructField("i_class", StringType,true),
                createStructField("i_category_id", IntegerType,true),
                createStructField("i_category", StringType,true),
                createStructField("i_manufact_id", IntegerType,true),
                createStructField("i_manufact", StringType,true),
                createStructField("i_size", StringType,true),
                createStructField("i_formulation", StringType,true),
                createStructField("i_color", StringType,true),
                createStructField("i_units", StringType,true),
                createStructField("i_container", StringType,true),
                createStructField("i_manager_id", IntegerType,true),
                createStructField("i_product_name", StringType,true)
        });
        StructType promotion = DataTypes.createStructType(new StructField[]{
                createStructField("p_promo_sk", IntegerType,true),
                createStructField("p_promo_id", StringType,true),
                createStructField("p_start_date_sk", IntegerType,true),
                createStructField("p_end_date_sk", IntegerType,true),
                createStructField("p_item_sk", IntegerType,true),
                createStructField("p_cost", DoubleType,true),
                createStructField("p_response_target", IntegerType,true),
                createStructField("p_promo_name", StringType,true),
                createStructField("p_channel_dmail", StringType,true),
                createStructField("p_channel_email", StringType,true),
                createStructField("p_channel_catalog", StringType,true),
                createStructField("p_channel_tv", StringType,true),
                createStructField("p_channel_radio", StringType,true),
                createStructField("p_channel_press", StringType,true),
                createStructField("p_channel_event", StringType,true),
                createStructField("p_channel_demo", StringType,true),
                createStructField("p_channel_details", StringType,true),
                createStructField("p_purpose", StringType,true),
                createStructField("p_discount_active", StringType,true),

        });
        StructType reason = DataTypes.createStructType(new StructField[]{
                createStructField("r_reason_sk", IntegerType,true),
                createStructField("r_reason_id", StringType,true),
                createStructField("r_reason_desc", StringType,true),
        });
        StructType ship_mode = DataTypes.createStructType(new StructField[]{
                createStructField("sm_ship_mode_sk", IntegerType,true),
                createStructField("sm_ship_mode_id", StringType,true),
                createStructField("sm_type", StringType,true),
                createStructField("sm_code", StringType,true),
                createStructField("sm_carrier", StringType,true),
                createStructField("sm_contract", StringType,true),

        });
        StructType store = DataTypes.createStructType(new StructField[]{
                createStructField("s_store_sk", IntegerType,true),
                createStructField("s_store_id", StringType,true),
                createStructField("s_rec_start_date", StringType,true),
                createStructField("s_rec_end_date", StringType,true),
                createStructField("s_closed_date_sk", IntegerType,true),
                createStructField("s_store_name", StringType,true),
                createStructField("s_number_employees", IntegerType,true),
                createStructField("s_floor_space", IntegerType,true),
                createStructField("s_hours", StringType,true),
                createStructField("s_manager", StringType,true),
                createStructField("s_market_id", IntegerType,true),
                createStructField("s_geography_class", StringType,true),
                createStructField("s_market_desc", StringType,true),
                createStructField("s_market_manager", StringType,true),
                createStructField("s_division_id", IntegerType,true),
                createStructField("s_division_name", StringType,true),
                createStructField("s_company_id", IntegerType,true),
                createStructField("s_company_name", StringType,true),
                createStructField("s_street_number", StringType,true),
                createStructField("s_street_name", StringType,true),
                createStructField("s_street_type", StringType,true),
                createStructField("s_suite_number", StringType,true),
                createStructField("s_city", StringType,true),
                createStructField("s_county", StringType,true),
                createStructField("s_state", StringType,true),
                createStructField("s_zip", StringType,true),
                createStructField("s_country", StringType,true),
                createStructField("s_gmt_offset", DoubleType,true),
                createStructField("s_tax_precentage", DoubleType,true),
        });
        StructType store_returns = DataTypes.createStructType(new StructField[]{
                createStructField("sr_returned_date_sk", IntegerType,true),
                createStructField("sr_return_time_sk", IntegerType,true),
                createStructField("sr_item_sk", IntegerType,true),
                createStructField("sr_customer_sk", IntegerType,true),
                createStructField("sr_cdemo_sk", IntegerType,true),
                createStructField("sr_hdemo_sk", IntegerType,true),
                createStructField("sr_addr_sk", IntegerType,true),
                createStructField("sr_store_sk", IntegerType,true),
                createStructField("sr_reason_sk", IntegerType,true),
                createStructField("sr_ticket_number", IntegerType,true),
                createStructField("sr_return_quantity", IntegerType,true),
                createStructField("sr_return_amt", DoubleType,true),
                createStructField("sr_return_tax", DoubleType,true),
                createStructField("sr_return_amt_inc_tax", DoubleType,true),
                createStructField("sr_fee", DoubleType,true),
                createStructField("sr_return_ship_cost", DoubleType,true),
                createStructField("sr_refunded_cash", DoubleType,true),
                createStructField("sr_reversed_charge", DoubleType,true),
                createStructField("sr_store_credit", DoubleType,true),
                createStructField("sr_net_loss", DoubleType,true),


        });
        StructType store_sales = DataTypes.createStructType(new StructField[]{ 
                createStructField("ss_sold_date_sk", IntegerType,true),
                createStructField("ss_sold_time_sk", IntegerType,true),
                createStructField("ss_item_sk", IntegerType,true),
                createStructField("ss_customer_sk", IntegerType,true),
                createStructField("ss_cdemo_sk", IntegerType,true),
                createStructField("ss_hdemo_sk", IntegerType,true),
                createStructField("ss_addr_sk", IntegerType,true),
                createStructField("ss_store_sk", IntegerType,true),
                createStructField("ss_promo_sk", IntegerType,true),
                createStructField("ss_ticket_number", IntegerType,true),
                createStructField("ss_quantity", IntegerType,true),
                createStructField("ss_wholesale_cost", DoubleType,true),
                createStructField("ss_list_price", DoubleType,true),
                createStructField("ss_sales_price", DoubleType,true),
                createStructField("ss_ext_discount_amt", DoubleType,true),
                createStructField("ss_ext_sales_price", DoubleType,true),
                createStructField("ss_ext_wholesale_cost", DoubleType,true),
                createStructField("ss_ext_list_price", DoubleType,true),
                createStructField("ss_ext_tax", DoubleType,true),
                createStructField("ss_coupon_amt", DoubleType,true),
                createStructField("ss_net_paid", DoubleType,true),
                createStructField("ss_net_paid_inc_tax", DoubleType,true),
                createStructField("ss_net_profit", DoubleType,true),

        });
        StructType time_dim = DataTypes.createStructType(new StructField[]{ 
                createStructField("t_time_sk", IntegerType,true),
                createStructField("t_time_id", StringType,true),
                createStructField("t_time", IntegerType,true),
                createStructField("t_hour", IntegerType,true),
                createStructField("t_minute", IntegerType,true),
                createStructField("t_second", IntegerType,true),
                createStructField("t_am_pm", StringType,true),
                createStructField("t_shift", StringType,true),
                createStructField("t_sub_shift", StringType,true),
                createStructField("t_meal_time", StringType,true),

        });
        StructType warehouse = DataTypes.createStructType(new StructField[]{ 
                createStructField("w_warehouse_sk", IntegerType,true),
                createStructField("w_warehouse_id", StringType,true),
                createStructField("w_warehouse_name", StringType,true),
                createStructField("w_warehouse_sq_ft", IntegerType,true),
                createStructField("w_street_number", StringType,true),
                createStructField("w_street_name", StringType,true),
                createStructField("w_street_type", StringType,true),
                createStructField("w_suite_number", StringType,true),
                createStructField("w_city", StringType,true),
                createStructField("w_county", StringType,true),
                createStructField("w_state", StringType,true),
                createStructField("w_zip", StringType,true),
                createStructField("w_country", StringType,true),
                createStructField("w_gmt_offset", DoubleType,true),

        });
        StructType web_page = DataTypes.createStructType(new StructField[]{ 
                createStructField("wp_web_page_sk", IntegerType,true),
                createStructField("wp_web_page_id", StringType,true),
                createStructField("wp_rec_start_date", StringType,true),
                createStructField("wp_rec_end_date", StringType,true),
                createStructField("wp_creation_date_sk", IntegerType,true),
                createStructField("wp_access_date_sk", IntegerType,true),
                createStructField("wp_autogen_flag", StringType,true),
                createStructField("wp_customer_sk", IntegerType,true),
                createStructField("wp_url", StringType,true),
                createStructField("wp_type", StringType,true),
                createStructField("wp_char_count", IntegerType,true),
                createStructField("wp_link_count", IntegerType,true),
                createStructField("wp_image_count", IntegerType,true),
                createStructField("wp_max_ad_count", IntegerType,true),

        });
        StructType web_returns = DataTypes.createStructType(new StructField[]{ 
                createStructField("wr_returned_date_sk", IntegerType,true),
                createStructField("wr_returned_time_sk", IntegerType,true),
                createStructField("wr_item_sk", IntegerType,true),
                createStructField("wr_refunded_customer_sk", IntegerType,true),
                createStructField("wr_refunded_cdemo_sk", IntegerType,true),
                createStructField("wr_refunded_hdemo_sk", IntegerType,true),
                createStructField("wr_refunded_addr_sk", IntegerType,true),
                createStructField("wr_returning_customer_sk", IntegerType,true),
                createStructField("wr_returning_cdemo_sk", IntegerType,true),
                createStructField("wr_returning_hdemo_sk", IntegerType,true),
                createStructField("wr_returning_addr_sk", IntegerType,true),
                createStructField("wr_web_page_sk", IntegerType,true),
                createStructField("wr_reason_sk", IntegerType,true),
                createStructField("wr_order_number", IntegerType,true),
                createStructField("wr_return_quantity", IntegerType,true),
                createStructField("wr_return_amt", DoubleType,true),
                createStructField("wr_return_tax", DoubleType,true),
                createStructField("wr_return_amt_inc_tax", DoubleType,true),
                createStructField("wr_fee", DoubleType,true),
                createStructField("wr_return_ship_cost", DoubleType,true),
                createStructField("wr_refunded_cash", DoubleType,true),
                createStructField("wr_reversed_charge", DoubleType,true),
                createStructField("wr_account_credit", DoubleType,true),
                createStructField("wr_net_loss", DoubleType,true),

        });
        StructType web_sales = DataTypes.createStructType(new StructField[]{
                createStructField("ws_sold_date_sk", IntegerType,true),
                createStructField("ws_sold_time_sk", IntegerType,true),
                createStructField("ws_ship_date_sk", IntegerType,true),
                createStructField("ws_item_sk", IntegerType,true),
                createStructField("ws_bill_customer_sk", IntegerType,true),
                createStructField("ws_bill_cdemo_sk", IntegerType,true),
                createStructField("ws_bill_hdemo_sk", IntegerType,true),
                createStructField("ws_bill_addr_sk", IntegerType,true),
                createStructField("ws_ship_customer_sk", IntegerType,true),
                createStructField("ws_ship_cdemo_sk", IntegerType,true),
                createStructField("ws_ship_hdemo_sk", IntegerType,true),
                createStructField("ws_ship_addr_sk", IntegerType,true),
                createStructField("ws_web_page_sk", IntegerType,true),
                createStructField("ws_web_site_sk", IntegerType,true),
                createStructField("ws_ship_mode_sk", IntegerType,true),
                createStructField("ws_warehouse_sk", IntegerType,true),
                createStructField("ws_promo_sk", IntegerType,true),
                createStructField("ws_order_number", IntegerType,true),
                createStructField("ws_quantity", IntegerType,true),
                createStructField("ws_wholesale_cost", DoubleType,true),
                createStructField("ws_list_price", DoubleType,true),
                createStructField("ws_sales_price", DoubleType,true),
                createStructField("ws_ext_discount_amt", DoubleType,true),
                createStructField("ws_ext_sales_price", DoubleType,true),
                createStructField("ws_ext_wholesale_cost", DoubleType,true),
                createStructField("ws_ext_list_price", DoubleType,true),
                createStructField("ws_ext_tax", DoubleType,true),
                createStructField("ws_coupon_amt", DoubleType,true),
                createStructField("ws_ext_ship_cost", DoubleType,true),
                createStructField("ws_net_paid", DoubleType,true),
                createStructField("ws_net_paid_inc_tax", DoubleType,true),
                createStructField("ws_net_paid_inc_ship", DoubleType,true),
                createStructField("ws_net_paid_inc_ship_tax", DoubleType,true),
                createStructField("ws_net_profit", DoubleType,true),

        });
        StructType web_site = DataTypes.createStructType(new StructField[]{
                createStructField("web_site_sk", IntegerType,true),
                createStructField("web_site_id", StringType,true),
                createStructField("web_rec_start_date", StringType,true),
                createStructField("web_rec_end_date", StringType,true),
                createStructField("web_name", StringType,true),
                createStructField("web_open_date_sk", IntegerType,true),
                createStructField("web_close_date_sk", IntegerType,true),
                createStructField("web_class", StringType,true),
                createStructField("web_manager", StringType,true),
                createStructField("web_mkt_id", IntegerType,true),
                createStructField("web_mkt_class", StringType,true),
                createStructField("web_mkt_desc", StringType,true),
                createStructField("web_market_manager", StringType,true),
                createStructField("web_company_id", IntegerType,true),
                createStructField("web_company_name", StringType,true),
                createStructField("web_street_number", StringType,true),
                createStructField("web_street_name", StringType,true),
                createStructField("web_street_type", StringType,true),
                createStructField("web_suite_number", StringType,true),
                createStructField("web_city", StringType,true),
                createStructField("web_county", StringType,true),
                createStructField("web_state", StringType,true),
                createStructField("web_zip", StringType,true),
                createStructField("web_country", StringType,true),
                createStructField("web_gmt_offset", DoubleType,true),
                createStructField("web_tax_percentage", DoubleType,true),

        });

        spark.read().format("csv").schema(call_center).option("delimiter", "|").load(tblReadPath + "call_center.dat").write().mode("overwrite").parquet(writeParquetPath+"call_center");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(catalog_page).option("delimiter", "|").load(tblReadPath + "catalog_page.dat").write().mode("overwrite").parquet(writeParquetPath+"catalog_page");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(catalog_returns).option("delimiter", "|").load(tblReadPath + "catalog_returns.dat").write().mode("overwrite").parquet(writeParquetPath+"catalog_returns");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(catalog_sales).option("delimiter", "|").load(tblReadPath + "catalog_sales.dat").write().mode("overwrite").parquet(writeParquetPath+"catalog_sales");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(customer).option("delimiter", "|").load(tblReadPath + "customer.dat").write().mode("overwrite").parquet(writeParquetPath+"customer");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(customer_address).option("delimiter", "|").load(tblReadPath + "customer_address.dat").write().mode("overwrite").parquet(writeParquetPath+"customer_address");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(customer_demographics).option("delimiter", "|").load(tblReadPath + "customer_demographics.dat").write().mode("overwrite").parquet(writeParquetPath+"customer_demographics");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(date_dim).option("delimiter", "|").load(tblReadPath + "date_dim.dat").write().mode("overwrite").parquet(writeParquetPath+"date_dim");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(household_demographics).option("delimiter", "|").load(tblReadPath + "household_demographics.dat").write().mode("overwrite").parquet(writeParquetPath+"household_demographics");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(income_band).option("delimiter", "|").load(tblReadPath + "income_band.dat").write().mode("overwrite").parquet(writeParquetPath+"income_band");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(inventory).option("delimiter", "|").load(tblReadPath + "inventory.dat").write().mode("overwrite").parquet(writeParquetPath+"inventory");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(item).option("delimiter", "|").load(tblReadPath + "item.dat").write().mode("overwrite").parquet(writeParquetPath+"item");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(promotion).option("delimiter", "|").load(tblReadPath + "promotion.dat").write().mode("overwrite").parquet(writeParquetPath+"promotion");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(reason).option("delimiter", "|").load(tblReadPath + "reason.dat").write().mode("overwrite").parquet(writeParquetPath+"reason");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(ship_mode).option("delimiter", "|").load(tblReadPath + "ship_mode.dat").write().mode("overwrite").parquet(writeParquetPath+"ship_mode");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(store).option("delimiter", "|").load(tblReadPath + "store.dat").write().mode("overwrite").parquet(writeParquetPath+"store");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(store_returns).option("delimiter", "|").load(tblReadPath + "store_returns.dat").write().mode("overwrite").parquet(writeParquetPath+"store_returns");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(store_sales).option("delimiter", "|").load(tblReadPath + "store_sales.dat").write().mode("overwrite").parquet(writeParquetPath+"store_sales");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(time_dim).option("delimiter", "|").load(tblReadPath + "time_dim.dat").write().mode("overwrite").parquet(writeParquetPath+"time_dim");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(warehouse).option("delimiter", "|").load(tblReadPath + "warehouse.dat").write().mode("overwrite").parquet(writeParquetPath+"warehouse");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(web_page).option("delimiter", "|").load(tblReadPath + "web_page.dat").write().mode("overwrite").parquet(writeParquetPath+"web_page");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(web_returns).option("delimiter", "|").load(tblReadPath + "web_returns.dat").write().mode("overwrite").parquet(writeParquetPath+"web_returns");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(web_sales).option("delimiter", "|").load(tblReadPath + "web_sales.dat").write().mode("overwrite").parquet(writeParquetPath+"web_sales");
        System.out.println("table wrote in parquet");
        spark.read().format("csv").schema(web_site).option("delimiter", "|").load(tblReadPath + "web_site.dat").write().mode("overwrite").parquet(writeParquetPath+"web_site");
    }

    public static void main(String[] args) {

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


        Dataset<Row> dataset = spark.read().parquet(ParquetPath +"call_center");
        dataset.registerTempTable("CALL_CENTER");
        dataset = spark.read().parquet(ParquetPath +"catalog_page");
        dataset.registerTempTable("CATALOG_PAGE");
        dataset = spark.read().parquet(ParquetPath +"catalog_returns");
        dataset.registerTempTable("CATALOG_RETURNS");
        dataset = spark.read().parquet(ParquetPath +"catalog_sales");
        dataset.registerTempTable("CATALOG_SALES");
        dataset = spark.read().parquet(ParquetPath +"customer");
        dataset.registerTempTable("CUSTOMER");
        dataset = spark.read().parquet(ParquetPath +"customer_address");
        dataset.registerTempTable("CUSTOMER_ADDRESS");
        dataset = spark.read().parquet(ParquetPath +"customer_demographics");
        dataset.registerTempTable("CUSTOMER_DEMOGRAPHICS");
        dataset = spark.read().parquet(ParquetPath +"date_dim");
        dataset.registerTempTable("DATE_DIM");
        dataset = spark.read().parquet(ParquetPath +"household_demographics");
        dataset.registerTempTable("HOUSEHOLD_DEMOGRAPHICS");
        dataset = spark.read().parquet(ParquetPath +"income_band");
        dataset.registerTempTable("INCOME_BAND");
        dataset = spark.read().parquet(ParquetPath +"inventory");
        dataset.registerTempTable("INVENTORY");
        dataset = spark.read().parquet(ParquetPath +"item");
        dataset.registerTempTable("ITEM");
        dataset = spark.read().parquet(ParquetPath +"promotion");
        dataset.registerTempTable("PROMOTION");
        dataset = spark.read().parquet(ParquetPath +"reason");
        dataset.registerTempTable("REASON");
        dataset = spark.read().parquet(ParquetPath +"ship_mode");
        dataset.registerTempTable("SHIP_MODE");
        dataset = spark.read().parquet(ParquetPath +"store");
        dataset.registerTempTable("STORE");
        dataset = spark.read().parquet(ParquetPath +"store_returns");
        dataset.registerTempTable("STORE_RETURNS");
        dataset = spark.read().parquet(ParquetPath +"store_sales");
        dataset.registerTempTable("STORE_SALES");
        dataset = spark.read().parquet(ParquetPath +"time_dim");
        dataset.registerTempTable("TIME_DIM");
        dataset = spark.read().parquet(ParquetPath +"warehouse");
        dataset.registerTempTable("WAREHOUSE");
        dataset = spark.read().parquet(ParquetPath +"web_page");
        dataset.registerTempTable("WEB_PAGE");
        dataset = spark.read().parquet(ParquetPath +"web_returns");
        dataset.registerTempTable("WEB_RETURNS");
        dataset = spark.read().parquet(ParquetPath +"web_sales");
        dataset.registerTempTable("WEB_SALES");
        dataset = spark.read().parquet(ParquetPath +"web_site");
        dataset.registerTempTable("WEB_SITE");


        ArrayList<Long> runTimes = new ArrayList<Long>();
        String[] queriesNumbersArray = queriesNumbers.split(",");
        for (int i = 0; i < queriesNumbersArray.length; i++) {
            StringBuilder sb = new StringBuilder();
            String line = null;
            BufferedReader bufferedReader = null;
            try {
                bufferedReader = new BufferedReader(
                        new FileReader(sqlQueriesPath + "query" + queriesNumbersArray[i] + ".sql")
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
                if (!line.contains("--"))
                    sb.append(line).append(" ");
            }
            String[] queries = sb.toString().split(";");
            for (int j = 0; j <queries.length-1 ; j++) {
                runBenchmarkQuery(queries[j],"RUN query number "+ queriesNumbersArray[i],spark,runTimes , logsPath);
            }
        }

        //end time
        long totalRunTime = 0;
        for (Long runTime : runTimes) {
            totalRunTime += runTime;
        }
        System.out.println("Runtime Of all Queries:" + totalRunTime + "seconds");

    }
}

