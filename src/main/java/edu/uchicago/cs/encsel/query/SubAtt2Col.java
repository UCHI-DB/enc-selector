package edu.uchicago.cs.encsel.query;

import java.io.*;

public class SubAtt2Col {

    public static void main(String[] args) throws IOException {
        String directory = System.getProperty("user.home");
        String fileName = "sample.txt";
        String absolutePath = directory + File.separator + fileName;

        args = new String[]{"../datasets/tpc-ds/customer_address.dat", "../datasets/tpc-ds/customer_address_merged.dat"};
        // write the content in file
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(args[1]));

        // read the content from file
        BufferedReader bufferedReader = new BufferedReader(new FileReader(args[0]));
        String line = bufferedReader.readLine();
        StringBuffer sline = new StringBuffer();
        while (line != null && line.length()>1) {
            String[] items = line.split("\\|", -1);
            assert items.length == 13;
            for (int i=0; i<items.length; i++){
                if (i<2){
                    sline.append(items[i]+"|");
                }
                else if (i<11){
                    sline.append(items[i].trim()+" ");
                }
                else{
                    sline.append("|"+items[i]);
                }

            }
            bufferedWriter.write(sline.toString()+"\n");
            sline.delete(0,sline.length());
            line = bufferedReader.readLine();
        }
        bufferedReader.close();
        bufferedWriter.flush();
        bufferedWriter.close();
    }

}
