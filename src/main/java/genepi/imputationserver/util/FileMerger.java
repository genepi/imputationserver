package genepi.imputationserver.util;

import genepi.io.text.LineReader;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileMerger {

	public static void mergeAndGz(String local, String hdfs, boolean removeHeader,
			String ext) throws IOException {

		GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(local));

		Configuration conf = new Configuration();

		FileSystem fileSystem = FileSystem.get(conf);
		Path pathFolder = new Path(hdfs);
		FileStatus[] files = fileSystem.listStatus(pathFolder);

		List<String> filenames = new Vector<String>();

		if (files != null) {

			// filters by extension and sorts by filename
			for (FileStatus file : files) {
				if (!file.isDir()
						&& !file.getPath().getName().startsWith("_")
						&& (ext == null || file.getPath().getName()
								.endsWith(ext))) {
					filenames.add(file.getPath().toString());
				}
			}
			Collections.sort(filenames);

			boolean firstFile = true;

			for (String filename : filenames) {
				Path path = new Path(filename);

				FSDataInputStream in = fileSystem.open(path);

				LineReader reader = new LineReader(in);

				boolean header = true;
				while (reader.next()) {

					String line = reader.get();

					if (removeHeader) {

						if (header) {
							if (firstFile) {
								out.write(line.toString().getBytes());
								firstFile = false;
							}
							header = false;
						} else {
							out.write('\n');
							out.write(line.toString().getBytes());
						}

					} else {

						if (header) {
							if (firstFile) {
								firstFile = false;
							} else {
								out.write('\n');
							}
							header = false;
						} else {
							out.write('\n');

						}
						out.write(line.toString().getBytes());
					}
				}

				in.close();

			}

			out.close();
		}

	}

}
