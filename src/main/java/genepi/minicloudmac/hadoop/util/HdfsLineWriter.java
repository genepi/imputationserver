package genepi.minicloudmac.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsLineWriter {

	private FSDataOutputStream out;

	private boolean first = true;

	public HdfsLineWriter(String filename) throws IOException {

		Configuration configuration = new Configuration();
		FileSystem filesystem = FileSystem.get(configuration);

		out = filesystem.create(new Path(filename));

	}

	public void write(String line) throws IOException {
		if (first) {
			out.writeBytes(line);
			first = false;
		} else {
			out.writeBytes("\n" + line);
		}
	}

	public void close() throws IOException {
		out.close();
	}

}
