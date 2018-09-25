package org.nygenome.als.graphdb.util;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

public abstract class RemoteRecordSupplier {
  protected JSch jsch;
 protected Session session;
 protected String sftpServer;

  protected Reader initRemoteReader(String fileName){
    try {
      session = jsch.getSession(
          FrameworkPropertyService.INSTANCE.getStringProperty("sftp_user"),
          sftpServer,
          22);
      session.setConfig("StrictHostKeyChecking", "no");
      session.setPassword(FrameworkPropertyService.INSTANCE.getStringProperty("sftp_password"));
      session.connect();

      Channel channel = session.openChannel("sftp");
      channel.connect();
      ChannelSftp sftpChannel = (ChannelSftp) channel;
      String remoteFileName = FrameworkPropertyService.INSTANCE.getStringProperty("sftp_als_data_dir")
          + fileName;
      InputStream stream = sftpChannel.get(remoteFileName);

      return new BufferedReader((new InputStreamReader(stream)));
    } catch (Exception e) {
      AsyncLoggingService.logError(e.getMessage());
      e.printStackTrace();
    }
    return null;
  }



}
