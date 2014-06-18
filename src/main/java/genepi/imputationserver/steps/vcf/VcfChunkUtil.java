package genepi.imputationserver.steps.vcf;

import genepi.hadoop.HdfsUtil;
import genepi.hadoop.io.HdfsLineWriter;
import genepi.io.text.LineReader;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

public class VcfChunkUtil {

	public static List<VcfChunk> mergeExcludedChunks(List<VcfChunk> chunks)
			throws IOException {

		List<VcfChunk> result = new Vector<VcfChunk>();

		for (int i = 0; i < chunks.size(); i++) {

			VcfChunk chunk = chunks.get(i);

			double overlap = chunk.getInReference() / (double) chunk.getSnps();

			if (overlap > 0.5 && chunk.getSnps() > 3) {

				result.add(chunk);

			} else {

				// merge

				if (i > 0) {

					VcfChunk prev = result.get(result.size() - 1);
					int newSnps = prev.getSnps() + chunk.getSnps();
					double newOverlap = (prev.getInReference() + chunk
							.getInReference()) / (double) newSnps;

					if (newOverlap > 0.5 && newSnps > 3) {

						String vcfFilename = prev.getVcfFilename() + "_"
								+ chunk.toString();

						// merge vcf-file
						LineReader readerPrev = new LineReader(
								HdfsUtil.open(prev.getVcfFilename()));
						LineReader readerChunk = new LineReader(
								HdfsUtil.open(chunk.getVcfFilename()));

						HdfsLineWriter writer = new HdfsLineWriter(vcfFilename);

						// first file
						while (readerPrev.next()) {
							String line = readerPrev.get();
							String tiles[] = line.split("\t", 3);
							int position = Integer.parseInt(tiles[1]);
							if (position <= prev.getEnd()) {
								writer.write(line);
							}
						}
						readerPrev.close();

						// second file
						while (readerChunk.next()) {
							String line = readerChunk.get();
							if (!line.startsWith("#")) {
								String tiles[] = line.split("\t", 3);
								int position = Integer.parseInt(tiles[1]);
								if (position >= chunk.getStart()) {
									writer.write(line);
								}
							}
						}
						readerChunk.close();
						writer.close();

						// extend it
						prev.setEnd(chunk.getEnd());
						prev.setSnps(newSnps);
						prev.setInReference(prev.getInReference()
								+ chunk.getInReference());
						prev.setVcfFilename(vcfFilename);

					} else {
						// exclude it!
					}

				}

			}

		}

		return result;

	}
}
