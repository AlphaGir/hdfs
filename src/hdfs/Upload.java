package hdfs;

import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Upload {

	public static void main(String[] args) throws IOException, URISyntaxException {
		// TODO Auto-generated method stub
		String source = "E://READEME.txt";
		         // hdfs文件路径
		         String dest = "hdfs://cdh.data4industry.com:8020/newdir/";
		         copyFromLocal(source, dest);
	}
	  public static void copyFromLocal(String source, String dest)throws IOException, URISyntaxException {
	        // 读取hadoop文件系统的配置
	        Configuration conf = new Configuration();
	        URI uri = new URI("hdfs://cdh.data4industry.com:8020");
	        // FileSystem是用户操作HDFS的核心类，它获得URI对应的HDFS文件系统
	        FileSystem fileSystem = FileSystem.get(uri, conf);
	        // 源文件路径
	        Path srcPath = new Path(source);
	        // 目的路径
	        Path dstPath = new Path(dest);
	        // 查看目的路径是否存在
	        if (!(((FileSystem) fileSystem).exists(dstPath))) {
	            // 如果路径不存在，即刻创建
	            fileSystem.mkdirs(dstPath);
	        }
	        // 得到本地文件名称
	        String filename = source.substring(source.lastIndexOf('/') + 1,source.length());
	        try {
	            // 将本地文件上传到HDFS
	            ((FileSystem) fileSystem).copyFromLocalFile(srcPath, dstPath);
	            System.out.println("File " + filename + " copied to " + dest);
	        } catch (Exception e) {
	            System.err.println("Exception caught! :" + e);
	            System.exit(1);
	        } finally {
	            fileSystem.close();
	        }
	    }

}
