package com.test.s3bucketlambdaexporter;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class Handler implements RequestHandler<Map<String, Object>, Object> {

  private static final String ZIP_FILE_EXTENSION = ".zip";
  private static final String DIRECTORY_SUFFIX_CHARACTER = "/";

  private static final AmazonS3 s3Client = AmazonS3Client.builder()
      .withCredentials(new DefaultAWSCredentialsProviderChain())
      .build();

  @SneakyThrows
  @Override
  public Object handleRequest(Map<String, Object> stringObjectMap, Context context) {
    final String bucket = (String) stringObjectMap.get("bucket");
    final String prefix = (String) stringObjectMap.get("prefix");
    final List<String> keys = getObjectsKeysFrom(bucket, prefix);
    ZipFile zipFile = null;
    String zipFileName = "";
    try {
      zipFile = downloadObjectsIntoZip(keys, bucket, prefix);
      zipFileName = zipFile.getFile().getName();
      upload(
          Files.readAllBytes(Paths.get(zipFile.getFile().getPath())),
          bucket,
          "archives/" + zipFile.getFile().getName(),
          "application/zip"
      );
    } catch (ZipException e) {
      log.error("Error while zipping file");
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (null != zipFile) {
        Path fileToDeletePath = Paths.get(zipFile.getFile().getPath());
        Files.delete(fileToDeletePath);
      } else {
        log.warn("Unexpected null zipFile");
      }
    }
    return zipFileName;
  }

  private static void upload(final byte[] content, final String bucketName, final String key,
      final String contentType) {
    val metadata = new ObjectMetadata();
    metadata.setContentType(contentType);
    metadata.setContentLength(content.length);
    val request = new PutObjectRequest(bucketName, key, new ByteArrayInputStream(content), metadata);
    s3Client.putObject(request);
    log.info("File {} uploaded to S3.", key);
  }

  private static List<String> getObjectsKeysFrom(final String bucketName, final String prefix) {
    val listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix);
    val objectKeys = new ArrayList<String>();
    ObjectListing objects = s3Client.listObjects(listObjectsRequest);
    while (Objects.nonNull(objects) && objects.getObjectSummaries() != null && !objects.getObjectSummaries()
        .isEmpty()) {
      objects.getObjectSummaries().forEach(s -> objectKeys.add(s.getKey()));
      objects = s3Client.listNextBatchOfObjects(objects);
    }
    return objectKeys.stream()
        .filter(k -> !k.equalsIgnoreCase(prefix))
        .collect(Collectors.toList());
  }

  public static ZipFile downloadObjectsIntoZip(final List<String> objectKeys, final String bucketName,
      final String objectKeysPrefix) throws ZipException {
    val zipFile = new ZipFile(getTempPathForFilename(objectKeysPrefix + ZIP_FILE_EXTENSION).toFile());
    log.info("Attempting to compress '{}' objects from '{}' into '{}'.", objectKeys, bucketName,
        zipFile.getFile().getName());
    addObjectsFromBucketWithPrefixIntoZip(objectKeys, bucketName, objectKeysPrefix, zipFile);
    log.info("Objects '{}' compressed into '{}'.", objectKeys, zipFile.getFile().getName());
    return zipFile;
  }

  private static void addObjectsFromBucketWithPrefixIntoZip(List<String> objectKeys, String bucketName,
      String objectKeysPrefix, ZipFile zipFile)
      throws ZipException {
    for (final String objectKey : objectKeys) {
      val filename = removeStart(objectKey, objectKeysPrefix + DIRECTORY_SUFFIX_CHARACTER);
      log.info("Attempting to download object '{}' from '{}'.", objectKey, bucketName);
      val s3File = s3Client.getObject(bucketName, objectKey);
      log.info("Object '{}' downloaded from '{}'.", objectKey, bucketName);
      log.info("Attempting to add object '{}' into '{}'.", objectKeys, zipFile.getFile().getName());
      zipFile.addStream(s3File.getObjectContent(), getZipParameters(filename));
      log.info("Object '{}' added into '{}'.", objectKeys, zipFile.getFile().getName());
    }
  }

  private static ZipParameters getZipParameters(String filename) {
    val parameters = new ZipParameters();
    parameters.setFileNameInZip(filename);
    return parameters;
  }

  private static Path getTempPathForFilename(final String fileName) {
    return new File(System.getProperty("java.io.tmpdir")).toPath().resolve(fileName);
  }

  private static String removeStart(final String str, final String remove) {
    if ((str == null || str.isEmpty()) || (remove == null || remove.isEmpty())) {
      return str;
    }
    if (str.startsWith(remove)) {
      return str.substring(remove.length());
    }
    return str;
  }
}
