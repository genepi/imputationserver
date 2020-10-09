package genepi.imputationserver.util;

import org.apache.commons.codec.binary.Hex;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Generate file checksums to be used for file integrity verification.
 */
public class FileChecksum {

    /**
     * Types of algorithms to be used for hashing.
     */
    public enum Algorithm {
        /**
         * MD5 hash
         */
        MD5("MD5"),
        /**
         * SHA-1 hash
         */
        SHA1("SHA-1"),
        /**
         * SHA-256 hash
         */
        SHA256("SHA-256");

        private final String type;

        Algorithm(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }

    /**
     * Returns a checksum based on the specified {@code Algorithm} type.
     *
     * @param file      the file for which a checksum will be created
     * @param algorithm the type of {@code Algorithm} requested.
     * @return A String of the file checksum
     * @throws IOException              if {@code file} is not found.
     * @throws NoSuchAlgorithmException if {@code algorithm} is not supported
     */
    public static String HashFile(File file, Algorithm algorithm) throws IOException, NoSuchAlgorithmException {
        try (FileInputStream inputStream = new FileInputStream(file)) {
            MessageDigest digest = MessageDigest.getInstance(algorithm.getType());

            byte[] bytesBuffer = new byte[4096];
            int bytesRead;

            while ((bytesRead = inputStream.read(bytesBuffer)) != -1) {
                digest.update(bytesBuffer, 0, bytesRead);
            }

            byte[] hashedBytes = digest.digest();

            return Hex.encodeHexString(hashedBytes);
        }
    }
}
