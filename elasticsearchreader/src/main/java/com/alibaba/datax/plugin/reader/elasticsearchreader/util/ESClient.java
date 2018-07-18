package com.alibaba.datax.plugin.reader.elasticsearchreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.elasticsearchreader.ESReaderErrorCode;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ESClient {

  private static final Logger LOG = LoggerFactory.getLogger(ESClient.class);

  /**
   *
   * @author Stone
   * @date 2018/4/10 15:16
   * @param host, port
   * @return org.elasticsearch.client.transport.TransportClient
   * @desc 获取到es的client连接
   *
   */
  public static TransportClient getClient(String host, String port) {

    Settings settings = Settings.builder().put("client.transport.ignore_cluster_name",true).build();
    TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(getTransportAddress(host,port));

    return client;
  }


  private static InetSocketTransportAddress getTransportAddress(String host, String port) {

    LOG.info(String.format("Connection details: host: %s. port:%s.", host, port));

    try {
      return new InetSocketTransportAddress(InetAddress.getByName(host), Integer.parseInt(port));

    } catch (UnknownHostException e) {

      throw DataXException.asDataXException(ESReaderErrorCode.BAD_CONFIG_VALUE,"请确认host和port的正确性");
    }
  }


}
