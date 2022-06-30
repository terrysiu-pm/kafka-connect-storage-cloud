/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.s3.partitioner;

import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class PMCustomPartitioner<T> extends TimeBasedPartitioner<T> {
  private static final Logger log = LoggerFactory.getLogger(PMCustomPartitioner.class);
  private long partitionDurationMs;
  private String pathFormat;
  private DateTimeFormatter formatter;

  private Set<String> headerFieldsForPartitions;

  public PMCustomPartitioner() {
    super();
  }

  private static DateTimeFormatter getDateTimeFormatter(String str, DateTimeZone timeZone) {
    return DateTimeFormat.forPattern(str).withZone(timeZone);
  }

  protected void init(long partitionDurationMs, String pathFormat, Locale locale,
                      DateTimeZone timeZone, Map<String, Object> config) {
    super.init(partitionDurationMs, pathFormat, locale, timeZone, config);
    this.delim = (String)config.get("directory.delim");
    this.partitionDurationMs = partitionDurationMs;
    this.pathFormat = pathFormat;

    try {
      this.formatter = getDateTimeFormatter(pathFormat, timeZone).withLocale(locale);
      this.timestampExtractor = this.newTimestampExtractor(
          (String)config.get("timestamp.extractor"));
      this.timestampExtractor.configure(config);
    } catch (IllegalArgumentException var9) {
      ConfigException ce = new ConfigException("path.format", pathFormat, var9.getMessage());
      ce.initCause(var9);
      throw ce;
    }
  }

  @Override
  public void configure(Map<String, Object> config) {
    super.configure(config);
    String headerPartitionFields = (String)config.get("header.partition.fields");
    String[] headerKeys = headerPartitionFields.split(",");
    headerFieldsForPartitions = new LinkedHashSet<>(headerKeys.length);
    for (String headerKey : headerKeys) {
      headerFieldsForPartitions.add(headerKey);
    }
  }

  public String encodePartition(SinkRecord sinkRecord, long nowInMillis) {
    Long timestamp = this.timestampExtractor.extract(sinkRecord, nowInMillis);
    return this.encodedPartitionForTimestamp(sinkRecord, timestamp);
  }

  public String encodePartition(SinkRecord sinkRecord) {
    Long timestamp = this.timestampExtractor.extract(sinkRecord);
    return this.encodedPartitionForTimestamp(sinkRecord, timestamp);
  }

  private String encodedPartitionForTimestamp(SinkRecord sinkRecord, Long timestamp) {
    if (timestamp == null) {
      String msg = "Unable to determine timestamp using timestamp.extractor "
          + this.timestampExtractor.getClass().getName() + " for record: " + sinkRecord;
      log.error(msg);
      throw new PartitionException(msg);
    } else {
      DateTime bucket = new DateTime(
          getPartition(this.getPartitionDurationMs(), timestamp, this.formatter.getZone()));
      Map<String, String> headers = new HashMap<>();
      for (Header header : sinkRecord.headers()) {
        headers.put(header.key(), header.value().toString());
      }
      String[] additionalHeaderPartitions = headerFieldsForPartitions
          .stream()
          .map(hdr -> String.format("%s=%s", hdr, headers.get(hdr)))
          .toArray(String[]::new);
      return String.format("%s/%s", String.join("/", additionalHeaderPartitions),
          bucket.toString(this.formatter));
    }
  }

  /*
  @Override
  public String generatePartitionedPath(String var1, String var2) {
    return String.format("title_id=%s", var1) + this.delim + var2;
  }
  */
}
