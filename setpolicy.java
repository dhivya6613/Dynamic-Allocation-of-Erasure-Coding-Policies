import java.io.*;
class dynamic{
public void set_dynamic_policy(int file_size){
  int blocksize=128;
  if(file_size<2.5*blocksize){
     		  System.out.println("--------------REPLICATION FACTOR IS SET AS 4-------------- ");
            String s7="hadoop dfs -setrep -w 4 -R /";
            Process p7 = Runtime.getRuntime().exec(s7);
            p7.waitFor();
            BufferedReader reader7 = new BufferedReader(
                        new InputStreamReader(p7.getInputStream()));
                String line7;
                while ((line7 = reader7.readLine()) != null) {
                    System.out.println(line7 + "\n");
                }

            reader7.close();


            System.out.println("--------------STORAGE ALLOCATION DETAILS WHEN REPLICATION FACTOR IS 4-------------- ");
            String s6="hdfs dfs -du -h /";
            Process p6 = Runtime.getRuntime().exec(s6);
            p6.waitFor();
            BufferedReader reader6 = new BufferedReader(
                        new InputStreamReader(p6.getInputStream()));
                String line6;
                while ((line6 = reader6.readLine()) != null) {
                    System.out.println(line6 + "\n");
                }

            reader6.close();
  }
 else{
   if(2.5* blocksize < file_size < 3* blocksize){
       String s2="hdfs ec -enablePolicy -policy RS-3-2-1024k";
            System.out.println("----------HADOOP ERASURE CODING POLICY RS-3-2-1024k IS ENABLED------------- ");
            Process p2 = Runtime.getRuntime().exec(s2);
            p2.waitFor();
            BufferedReader reader2 = new BufferedReader(
                        new InputStreamReader(p2.getInputStream()));
                String line2;
                while ((line2 = reader2.readLine()) != null) {
                    System.out.println(line2 + "\n");
                }

            reader2.close();

            String s3="hdfs ec -setPolicy -path /movies -policy RS-3-2-1024k";
            System.out.println("-------------RS-3-2-1024k ERASURE CODING POLICY IS SET------------------");
            Process p3 = Runtime.getRuntime().exec(s3);
            p3.waitFor();
            BufferedReader reader3 = new BufferedReader(
                        new InputStreamReader(p3.getInputStream()));
                String line3;
                while ((line3 = reader3.readLine()) != null) {
                    System.out.println(line3 + "\n");
                }

            reader3.close();
}
else{
if(3* blocksize < file_size < 6* blocksize){
          String s2="hdfs ec -enablePolicy -policy RS-6-3-1024k";
            System.out.println("----------HADOOP ERASURE CODING POLICY 6-3-1024k IS ENABLED------------- ");
            Process p2 = Runtime.getRuntime().exec(s2);
            p2.waitFor();
            BufferedReader reader2 = new BufferedReader(
                        new InputStreamReader(p2.getInputStream()));
                String line2;
                while ((line2 = reader2.readLine()) != null) {
                    System.out.println(line2 + "\n");
                }

            reader2.close();

            String s3="hdfs ec -setPolicy -path /my_dir_hdfs -policy RS-6-3-1024k";
            System.out.println("-------------6-3-1024k ERASURE CODING POLICY IS SET------------------");
            Process p3 = Runtime.getRuntime().exec(s3);
            p3.waitFor();
            BufferedReader reader3 = new BufferedReader(
                        new InputStreamReader(p3.getInputStream()));
                String line3;
                while ((line3 = reader3.readLine()) != null) {
                    System.out.println(line3 + "\n");
                }

            reader3.close();
}
 else{
            String s2="hdfs ec -enablePolicy -policy XOR-2-1-1024k";
            System.out.println("----------HADOOP ERASURE CODING POLICY XOR-2-1-1024k IS ENABLED------------- ");
            Process p2 = Runtime.getRuntime().exec(s2);
            p2.waitFor();
            BufferedReader reader2 = new BufferedReader(
                        new InputStreamReader(p2.getInputStream()));
                String line2;
                while ((line2 = reader2.readLine()) != null) {
                    System.out.println(line2 + "\n");
                }

            reader2.close();

            String s3="hdfs ec -setPolicy -path /my_dir_hdfs -policy XOR-2-1-1024k";
            System.out.println("-------------XOR-2-1-1024K ERASURE CODING POLICY IS SET------------------");
            Process p3 = Runtime.getRuntime().exec(s3);
            p3.waitFor();
            BufferedReader reader3 = new BufferedReader(
                        new InputStreamReader(p3.getInputStream()));
                String line3;
                while ((line3 = reader3.readLine()) != null) {
                    System.out.println(line3 + "\n");
                }

            reader3.close();
     }
  }  
 }
} 
}
class setpolicy{
public static void main(String a[]){
    dynamic d=new dynamic();
 try {  
            String s1="hdfs ec -listPolicies";
            System.out.println("-------------VARIOUS ERASURE CODING POLICY DETAILS------------------");
            Process p1 = Runtime.getRuntime().exec(s1);
            p1.waitFor();
            BufferedReader reader1 = new BufferedReader(
                        new InputStreamReader(p1.getInputStream()));
                String line1;
                while ((line1 = reader1.readLine()) != null) {
                    System.out.println(line1 + "\n");
                }

            reader1.close();

            String s2="hdfs ec -enablePolicy -policy XOR-2-1-1024k";
            System.out.println("----------HADOOP ERASURE CODING POLICY XOR-2-1-1024k IS ENABLED------------- ");
            Process p2 = Runtime.getRuntime().exec(s2);  
            p2.waitFor();
            BufferedReader reader2 = new BufferedReader(
                        new InputStreamReader(p2.getInputStream()));
                String line2;
                while ((line2 = reader2.readLine()) != null) {
                    System.out.println(line2 + "\n");
                }

            reader2.close();

            String s3="hdfs ec -setPolicy -path /my_dir_hdfs -policy XOR-2-1-1024k";
            System.out.println("-------------XOR-2-1-1024K ERASURE CODING POLICY IS SET------------------");
            Process p3 = Runtime.getRuntime().exec(s3);
            p3.waitFor();
            BufferedReader reader3 = new BufferedReader(
                        new InputStreamReader(p3.getInputStream()));
                String line3;
                while ((line3 = reader3.readLine()) != null) {
                    System.out.println(line3 + "\n");
                }

            reader3.close();
           
            System.out.println("--------------STORAGE ALLOCATION DETAILS WHILE USING THE ERASURE CODING POLICY XOR-2-1-1024K -------------- ");            String s4="hdfs dfsadmin -report";
            Process p4 = Runtime.getRuntime().exec(s4);
            p4.waitFor();
            BufferedReader reader4 = new BufferedReader(
                        new InputStreamReader(p4.getInputStream()));
                String line4;
                while ((line4 = reader4.readLine()) != null) {
                    System.out.println(line4 + "\n");
                }

            reader4.close();
              
            System.out.println("--------------STORAGE ALLOCATION DETAILS WHEN REPLICATION FACTOR IS 2-------------- "); 
            String s5="hdfs dfs -du -h /";
            Process p5 = Runtime.getRuntime().exec(s5);
            p5.waitFor();
            BufferedReader reader5 = new BufferedReader(
                        new InputStreamReader(p5.getInputStream()));
                String line5;
                while ((line5 = reader5.readLine()) != null) {
                    System.out.println(line5 + "\n");
                }

            reader5.close();
            
            System.out.println("--------------REPLICATION FACTOR IS SET AS 4-------------- ");
            String s7="hadoop dfs -setrep -w 4 -R /";
            Process p7 = Runtime.getRuntime().exec(s7);
            p7.waitFor();
            BufferedReader reader7 = new BufferedReader(
                        new InputStreamReader(p7.getInputStream()));
                String line7;
                while ((line7 = reader7.readLine()) != null) {
                    System.out.println(line7 + "\n");
                }

            reader7.close();


            System.out.println("--------------STORAGE ALLOCATION DETAILS WHEN REPLICATION FACTOR IS 4-------------- ");
            String s6="hdfs dfs -du -h /";
            Process p6 = Runtime.getRuntime().exec(s6);
            p6.waitFor();
            BufferedReader reader6 = new BufferedReader(
                        new InputStreamReader(p6.getInputStream()));
                String line6;
                while ((line6 = reader6.readLine()) != null) {
                    System.out.println(line6 + "\n");
                }

            reader6.close();

            //set the erasure policy dynamically

            System.out.println("--------------SETTING EC POLICY DYNAMICALLY-------------- ");
            String s8="file_size_kb=`du -k "/Users/chutki/Downloads/butta.webm" | cut -f1`";
            Process p8 = Runtime.getRuntime().exec(s8);
            p8.waitFor();
            String s8="echo $file_size_kb";
            Process p8 = Runtime.getRuntime().exec(s8);
            p8.waitFor();
            BufferedReader reader8 = new BufferedReader(
                        new InputStreamReader(p8.getInputStream()));
                String line8=reader8.readLine();
                int fsize=Integer.parseInt(line8);

            reader8.close();

            d.set_dynamic_policy(fsize);
            }  
        catch (Exception e) {  
            e.printStackTrace();  
            }  
    } 
  }

