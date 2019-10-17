package edu.uchicago.cs.encsel.adapter.carbondata;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.sdk.file.Field;
import org.junit.Test;

import java.io.IOException;
/**
 \* initial API and implementation.
 \* contributors: Chunwei Liu
 \* Date: 10/12/2018
 \* Time: 11:32 AM
 \*
 \*/
public class CarbonWriterReaderHelperTest{

    @Test
    public void testCSVParseAndWrite() throws IOException {
        String inputFile = "src/test/resources/CarbonSample.csv";
        String outputPath = "./target/testSdkWriter";
        Field[] fields = new Field[5];
        fields[0] = new Field("State", DataTypes.STRING);
        fields[1] = new Field("ZIP", DataTypes.INT);
        fields[2] = new Field("City", DataTypes.STRING);
        fields[3] = new Field("Address", DataTypes.STRING);
        fields[4] = new Field("Email", DataTypes.STRING);
        CarbonWriterHelper.CSVParseAndWrite(inputFile,outputPath,fields,",");
    }

    @Test
    public void testCarbonScan() throws IOException {
        //String inputFile = "./src/test/resource/carbonFile";
        String inputFile = "./target/testSdkWriter";
        Expression expression = new EqualToExpression(new ColumnExpression("Email", DataTypes.STRING),
                new LiteralExpression("aibrahim@gmail.com", DataTypes.STRING));
        Expression expCity = new EqualToExpression(new ColumnExpression("City", DataTypes.STRING),
                new LiteralExpression("Chicago", DataTypes.STRING));
        Expression exp = new AndExpression(expression,expCity);
        String[] stringPos = new String[]{"Email","City","Address"};
        CarbonReaderHelper.readCarbonfile(inputFile,stringPos,exp);
    }
}