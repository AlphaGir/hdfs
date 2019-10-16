package com.alphagir.bigdata;

import com.alphagir.bigdata.helper.HdfsApi;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class FlinkHdfsApplication {

    private String uri = "hdfs://sandbox.example.test:8020";
    private String user = "hdfs";

    private String uploadDir = "/tmp/sample.txt";

    public static void main(String[] args) {
        FlinkHdfsApplication application = new FlinkHdfsApplication();
        application.uploadToHdfs();
    }

    private void uploadToHdfs() {
        String source = "src/main/resources/sample.txt";

        try {
            File f = new File(source);
            InputStream is = new FileInputStream(f);
            HdfsApi api = new HdfsApi(this.uri, this.user);
            api.uploadFile(is, this.uploadDir);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
