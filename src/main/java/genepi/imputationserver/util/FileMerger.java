package genepi.imputationserver.util;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import genepi.hadoop.HdfsUtil;
import genepi.imputationserver.steps.imputation.ImputationPipeline;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.io.text.LineReader;
import htsjdk.samtools.util.BlockCompressedOutputStream;

public class FileMerger {

	public static final String R2_FLAG = "R2";

	public static void splitIntoHeaderAndData(String input, OutputStream outHeader, OutputStream outData,
			ImputationParameters parameters) throws IOException {
		LineReader reader = new LineReader(input);

		while (reader.next()) {
			String line = reader.get();
			if (!line.startsWith("#")) {
				if (parameters.getMinR2() > 0) {
					// rsq set. parse line and check rsq
					String info = parseInfo(line);
					if (info != null) {
						boolean keep = keepVcfLineByInfo(info, R2_FLAG, parameters.getMinR2());
						if (keep) {
							outData.write(line.getBytes());
							outData.write("\n".getBytes());
						}
					} else {
						// no valid vcf line. keep line
						outData.write(line.getBytes());
						outData.write("\n".getBytes());
					}
				} else {
					// no rsq set. keep all lines without parsing
					outData.write(line.getBytes());
					outData.write("\n".getBytes());
				}
			} else {

				// write filter command before ID List starting with #CHROM
				if (line.startsWith("#CHROM")) {
					outHeader.write(("##pipeline=" + ImputationPipeline.PIPELINE_VERSION + "\n").getBytes());
					outHeader.write(("##imputation=" + ImputationPipeline.IMPUTATION_VERSION + "\n").getBytes());
					outHeader.write(("##phasing=" + parameters.getPhasingMethod() + "\n").getBytes());
					outHeader.write(("##panel=" + parameters.getReferencePanelName() + "\n").getBytes());
					outHeader.write(("##r2Filter=" + parameters.getMinR2() + "\n").getBytes());
				}

				// write all headers except minimac4 command
				if (!line.startsWith("##minimac4_Command") && !line.startsWith("##source")) {
					outHeader.write(line.getBytes());
					outHeader.write("\n".getBytes());
				}
			}
		}
		outData.close();
		outHeader.close();
		reader.close();
	}

	public static void splitPhasedIntoHeaderAndData(String input, OutputStream outHeader, OutputStream outData,
			VcfChunk chunk, ImputationParameters parameters) throws IOException {
		LineReader reader = new LineReader(input);

		while (reader.next()) {
			String line = reader.get();
			if (!line.startsWith("#")) {
				// phased files also include phasingWindow
				int pos = Integer.valueOf(line.split("\t", 3)[1]);
				// no rsq set. keep all lines without parsing
				if (pos >= chunk.getStart() && pos <= chunk.getEnd()) {
					outData.write(line.getBytes());
					outData.write("\n".getBytes());
				}
			} else {

				// write filter command before ID List starting with #CHROM
				if (line.startsWith("#CHROM")) {
					outHeader.write(("##pipeline=" + ImputationPipeline.PIPELINE_VERSION + "\n").getBytes());
					outHeader.write(("##phasing=" + parameters.getPhasingMethod() + "\n").getBytes());
					outHeader.write(("##panel=" + parameters.getReferencePanelName() + "\n").getBytes());
				}

				// write all headers except eagle command
				if (!line.startsWith("##eagleCommand=eagle")) {
					outHeader.write(line.getBytes());
					outHeader.write("\n".getBytes());
				}
			}
		}
		outData.close();
		outHeader.close();
		reader.close();
	}

	public static class BgzipSplitOutputStream extends BlockCompressedOutputStream {

		public final static File emptyFile = null;

		public BgzipSplitOutputStream(OutputStream os) {
			super(os, (File) emptyFile);
		}

		@Override
		public void close() throws IOException {
			close(false);
		}

	}

	public static void mergeAndGzInfo(List<String> hdfs, String local) throws IOException {

		GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(local));

		boolean firstFile = true;

		for (String file : hdfs) {

			DataInputStream in = HdfsUtil.open(file);

			LineReader reader = new LineReader(in);

			boolean lineBreak = false;

			while (reader.next()) {

				String line = reader.get();

				if (line.startsWith("#")) {

					if (firstFile) {

						if (lineBreak) {
							out.write('\n');
						}
						out.write(line.toString().getBytes());
						lineBreak = true;
					}
				} else {
					out.write('\n');
					out.write(line.toString().getBytes());
				}
			}
			
			firstFile = false;
			
			in.close();

		}

		out.close();
	}

	public static boolean keepVcfLineByInfo(String info, String field, double value) {
		String[] tilesInfo = info.split(";");
		for (String tile : tilesInfo) {
			String[] tilesRsq = tile.split("=");
			if (tilesRsq.length == 2) {
				String id = tilesRsq[0];
				String stringValue = tilesRsq[1];
				if (id.equals(field)) {
					double doubleValue = Double.parseDouble(stringValue);
					return (doubleValue > value);
				}
			}
		}
		return true;
	}

	public static String parseInfo(String line) {
		// rsq set. parse line and check rsq in info
		String[] tiles = line.split("\t", 9);
		if (tiles.length == 9) {
			String info = tiles[7];
			return info;
		} else {
			return null;
		}
	}

}
