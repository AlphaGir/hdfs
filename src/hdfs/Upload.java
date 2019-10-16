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
		         // hdfs�ļ�·��
		         String dest = "hdfs://cdh.data4industry.com:8020/newdir/";
		         copyFromLocal(source, dest);
	}
	  public static void copyFromLocal(String source, String dest)throws IOException, URISyntaxException {
	        // ��ȡhadoop�ļ�ϵͳ������
	        Configuration conf = new Configuration();
	        URI uri = new URI("hdfs://cdh.data4industry.com:8020");
	        // FileSystem���û�����HDFS�ĺ����࣬�����URI��Ӧ��HDFS�ļ�ϵͳ
	        FileSystem fileSystem = FileSystem.get(uri, conf);
	        // Դ�ļ�·��
	        Path srcPath = new Path(source);
	        // Ŀ��·��
	        Path dstPath = new Path(dest);
	        // �鿴Ŀ��·���Ƿ����
	        if (!(((FileSystem) fileSystem).exists(dstPath))) {
	            // ���·�������ڣ����̴���
	            fileSystem.mkdirs(dstPath);
	        }
	        // �õ������ļ�����
	        String filename = source.substring(source.lastIndexOf('/') + 1,source.length());
	        try {
	            // �������ļ��ϴ���HDFS
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
