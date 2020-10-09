package genepi.imputationserver.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class FileChecksumTest {
    private static final String MD5CHECKSUM = "14fdae51731c8b9c15758b7b488ebc5e";
    private static final String SHA1CHECKSUM = "78af2bee1b95c6f8348b583240b11cd748a946f3";
    private static final String SHA256CHECKSUM = "2f342db135cdf887311c871c7eeb99f8c1587e6e248ae0ba953c12958fe1e396";
    private static final File MOCKVCF = new File("test-data/data/chr20-phased/chr20.R50.merged.1.330k.recode.small.vcf.gz");

    @Test
    public void md5Checksum() {
        try {
            String checksum = FileChecksum.HashFile(MOCKVCF, FileChecksum.Algorithm.MD5);
            Assert.assertEquals(MD5CHECKSUM, checksum);
        } catch (IOException | NoSuchAlgorithmException ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void sha1Checksum() {
        try {
            String checksum = FileChecksum.HashFile(MOCKVCF, FileChecksum.Algorithm.SHA1);
            Assert.assertEquals(SHA1CHECKSUM, checksum);
        } catch (IOException | NoSuchAlgorithmException ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void sha256Checksum() {
        try {
            String checksum = FileChecksum.HashFile(MOCKVCF, FileChecksum.Algorithm.SHA256);
            Assert.assertEquals(SHA256CHECKSUM, checksum);
        } catch (IOException | NoSuchAlgorithmException ex) {
            ex.printStackTrace();
        }
    }
}
