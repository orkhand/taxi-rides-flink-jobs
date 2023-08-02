/* (C)2023 */
package org.example;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.zip.GZIPInputStream;

public class AutoscalerMetricsDecoder {
  public static void main(String[] args) throws IOException {
    InputStream is =
        AutoscalerMetricsDecoder.class.getResourceAsStream("/autoscaler-metrics-from-configmap");

    assert is != null;
    BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
    String line;
    List<String> decodedLines = new ArrayList<>();
    while ((line = reader.readLine()) != null) {
      decodedLines.add(decodeBase64AndDecompress(line));
    }

    FileWriter writer = new FileWriter(getCurrentDateTime() + "-output.txt");
    for (String str : decodedLines) {
      writer.write(str + System.lineSeparator());
    }
    writer.close();
  }

  private static String decodeBase64AndDecompress(String line) throws IOException {
    byte[] decodedBytes = Base64.getDecoder().decode(line);
    return decompress(decodedBytes);
  }

  public static String decompress(final byte[] compressed) throws IOException {
    final StringBuilder outStr = new StringBuilder();
    if ((compressed == null) || (compressed.length == 0)) {
      return "";
    }
    if (isCompressed(compressed)) {
      final GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressed));
      final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gis, "UTF-8"));

      String line;
      while ((line = bufferedReader.readLine()) != null) {
        outStr.append(line);
      }
    } else {
      outStr.append(compressed);
    }
    return outStr.toString();
  }

  public static boolean isCompressed(final byte[] compressed) {
    return (compressed[0] == (byte) (GZIPInputStream.GZIP_MAGIC))
        && (compressed[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8));
  }

  private static String getCurrentDateTime() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
    return sdf.format(new Date());
  }
}
