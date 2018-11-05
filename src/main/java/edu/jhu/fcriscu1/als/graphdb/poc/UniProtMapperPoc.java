package edu.jhu.fcriscu1.als.graphdb.poc;

import org.apache.log4j.Logger;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

public class UniProtMapperPoc {
  private static final String UNIPROT_SERVER = "https://www.uniprot.org/";
  private static final Logger LOG = Logger.getLogger(UniProtMapperPoc.class);

  private static void run(String tool, ParameterNameValue[] params)
      throws Exception
  {
    StringBuilder locationBuilder = new StringBuilder(UNIPROT_SERVER + tool + "/?");
    for (int i = 0; i < params.length; i++)
    {
      if (i > 0)
        locationBuilder.append('&');
      locationBuilder.append(params[i].name).append('=').append(params[i].value);
    }
    String location = locationBuilder.toString();
    URL url = new URL(location);
    LOG.info("Submitting...");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    HttpURLConnection.setFollowRedirects(true);
    conn.setDoInput(true);
    conn.connect();

    int status = conn.getResponseCode();
    while (true)
    {
      int wait = 0;
      String header = conn.getHeaderField("Retry-After");
      if (header != null)
        wait = Integer.valueOf(header);
      if (wait == 0)
        break;
      LOG.info("Waiting (" + wait + ")...");
      conn.disconnect();
      Thread.sleep(wait * 1000);
      conn = (HttpURLConnection) new URL(location).openConnection();
      conn.setDoInput(true);
      conn.connect();
      status = conn.getResponseCode();
    }
    if (status == HttpURLConnection.HTTP_OK)
    {
      LOG.info("Got a OK reply");
      InputStream reader = conn.getInputStream();
      URLConnection.guessContentTypeFromStream(reader);
      StringBuilder builder = new StringBuilder();
      int a = 0;
      while ((a = reader.read()) != -1)
      {
        builder.append((char) a);
      }
      System.out.println(builder.toString());
    }
    else
      LOG.error("Failed, got " + conn.getResponseMessage() + " for "
          + location);
    conn.disconnect();
  }

  public static void main(String[] args)
      throws Exception
  {
    run("uploadlists", new ParameterNameValue[] {
        new ParameterNameValue("from", "GENENAME"),
        new ParameterNameValue("to", "ENSEMBL_TRS_ID"),
        new ParameterNameValue("format", "tab"),
        new ParameterNameValue("query", "LINC01937 BRCA1 GGT4P MARK2 LIX1"),
    });
  }

  private static class ParameterNameValue
  {
    private final String name;
    private final String value;

    public ParameterNameValue(String name, String value)
        throws UnsupportedEncodingException
    {
      this.name = URLEncoder.encode(name, "UTF-8");
      this.value = URLEncoder.encode(value, "UTF-8");
    }
  }
}
