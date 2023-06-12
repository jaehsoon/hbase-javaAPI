import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class hbaseQuery6 {

   public static void main(String args[]) throws IOException{
    // Get starting time
    long startTime = System.nanoTime();

    // Instantiating Configuration class
    Configuration config = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(config);

    // Instantiating HTable class
    Table table = connection.getTable(TableName.valueOf("superstore"));

    // Instantiating the Scan class
    Scan scan = new Scan();

    // Scanning the required columns
    scan.addColumn(Bytes.toBytes("customer"), Bytes.toBytes("Country"));
    scan.addColumn(Bytes.toBytes("product"), Bytes.toBytes("Sales"));

    // Getting the scan result
    ResultScanner scanner = table.getScanner(scan);

    // Instantiating HashMap to store country and total sales
    HashMap<String,Double> country_sales = new HashMap<String,Double>();

    for (Result result : scanner){

        byte[] countryBytes = result.getValue(Bytes.toBytes("customer"), Bytes.toBytes("Country"));
        byte[] salesBytes = result.getValue(Bytes.toBytes("product"), Bytes.toBytes("Sales"));

        if (countryBytes != null && salesBytes != null) {
            // Get country from row
            String country = Bytes.toString(countryBytes);
            
            // Skip iteration if country has value "country"
            if(country.equals("Country")){
                continue;
            }
            
            // Get sales from row
            String sales = Bytes.toString(salesBytes);
            Double salesDouble = Double.parseDouble(sales);

            // Put into HashMap
            if(country_sales.containsKey(country)){
                Double temp = country_sales.get(country);
                temp += salesDouble;
                country_sales.put(country, temp);
            }else{
                country_sales.put(country, salesDouble);
            }

            // Output current country and sales
            // System.out.println("Country : " + country + ", Sales : " + salesDouble);
        }
    }

    // get the maximum sales in the hashmap
    Double maxTotalSales = Collections.max(country_sales.values());
    //get the country with maximum sales
    String maxTotalSalesCountry = Collections.max(country_sales.entrySet(),Map.Entry.comparingByValue()).getKey();
    
    // Print the country with maximum total sale
    System.out.println(maxTotalSalesCountry + " has the highest total sales amount of: " + maxTotalSales);

    // closing the scanner
    scanner.close();
    // closing table
    table.close();

    // Get program ending time
    long endTime = System.nanoTime();
    System.out.println("Program duration: " + (endTime-startTime)/1000000 + " ms");
   }
}
