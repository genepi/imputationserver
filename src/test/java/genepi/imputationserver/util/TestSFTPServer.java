package genepi.imputationserver.util;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.sshd.SshServer;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.Session;
import org.apache.sshd.common.file.FileSystemView;
import org.apache.sshd.common.file.nativefs.NativeFileSystemFactory;
import org.apache.sshd.common.file.nativefs.NativeFileSystemView;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.PasswordAuthenticator;
import org.apache.sshd.server.command.ScpCommandFactory;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.sftp.subsystem.SftpSubsystem;

public class TestSFTPServer {

	private SshServer sshd;

	public static final String USERNAME = "username";

	public static final String PASSWORD = "password";

	public TestSFTPServer(final String rootFolder) throws IOException {

		sshd = SshServer.setUpDefaultServer();
		sshd.setFileSystemFactory(new NativeFileSystemFactory() {
			@Override
			public FileSystemView createFileSystemView(final Session session) {
				return new NativeFileSystemView(session.getUsername(), false) {
					@Override
					public String getVirtualUserDir() {
						System.out.println("Virtual Root: " + new File(rootFolder).getAbsolutePath());
						return new File(rootFolder).getAbsolutePath();
					}
					
			
				};
			};
		});
		sshd.setPort(8001);
		sshd.setSubsystemFactories(Arrays.<NamedFactory<Command>>asList(new SftpSubsystem.Factory()));
		sshd.setCommandFactory(new ScpCommandFactory());
		sshd.setKeyPairProvider(
				new SimpleGeneratorHostKeyProvider(new File(rootFolder + "/hostkey.ser").getAbsolutePath()));
		sshd.setPasswordAuthenticator(new PasswordAuthenticator() {
			@Override
			public boolean authenticate(final String username, final String password, final ServerSession session) {
				return StringUtils.equals(username, USERNAME) && StringUtils.equals(password, PASSWORD);
			}
		});
		sshd.start();

	}
	
	public void stop() throws InterruptedException{
		sshd.stop();
	}

}
