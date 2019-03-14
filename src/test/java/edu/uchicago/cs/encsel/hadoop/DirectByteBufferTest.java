package edu.uchicago.cs.encsel.hadoop;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;

import static org.junit.Assert.assertEquals;

public class DirectByteBufferTest {

    @Test
    public void testMap() throws Exception {
        URI localpath = new File("src/test/resource/query_select/customer_100").toURI();
        ByteArrayOutputStream memresult = new ByteArrayOutputStream();
        IOUtils.copy(new FileInputStream(new File(localpath)), memresult);

        byte[] content = memresult.toByteArray();
        DirectByteArray target = new DirectByteArray(content.length);
        target.from(new ByteArrayInputStream(content), content.length);

        for (int i = 0; i < memresult.size(); i++) {
            assertEquals(String.valueOf(i), content[i], target.get(i));
        }

        byte[] another = new byte[content.length];
        target.read(0, another, 0, another.length);

        for (int i = 0; i < memresult.size(); i++) {
            assertEquals(String.valueOf(i), content[i], another[i]);
        }
        target.destroy();
    }

    @Test
    public void testRead() throws Exception {
        URI localpath = new File("src/test/resource/query_select/customer_100").toURI();
        ByteArrayOutputStream memresult = new ByteArrayOutputStream();
        IOUtils.copy(new FileInputStream(new File(localpath)), memresult);

        byte[] content = memresult.toByteArray();
        DirectByteArray target = new DirectByteArray(content.length);
        target.from(new ByteArrayInputStream(content), content.length);

        byte[] buffer = new byte[1000];

        ByteArrayOutputStream anotherbuffer = new ByteArrayOutputStream();

        long pos = 0;
        int read = 0;
        while ((read = target.read(pos, buffer, 0, 1000)) > 0) {
            anotherbuffer.write(buffer, 0, read);
            pos += read;
        }

        byte[] anothercontent = anotherbuffer.toByteArray();
        assertEquals(content.length, anothercontent.length);

        for (int i = 0; i < memresult.size(); i++) {
            assertEquals(String.valueOf(i), content[i], anothercontent[i]);
        }
        target.destroy();
    }
}
