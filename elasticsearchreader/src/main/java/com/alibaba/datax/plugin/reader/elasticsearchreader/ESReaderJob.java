package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.elasticsearchreader.util.ESClient;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ESReaderJob {

  private static final Logger LOG = LoggerFactory.getLogger(ESReaderJob.class);

  private Configuration readerOriginConfig = null;
  private TransportClient client;


  public void init(Configuration readerOriginConfig){
    this.readerOriginConfig = readerOriginConfig;
    this.validate();

  }

  /**
   *
   * @author Stone
   * @date 2018/4/10 15:52
   * @return void
   * @desc  主要验证到ES的连接
   *
   */
  public void validate(){

    LOG.info("validate() begin...");

    String host = this.readerOriginConfig.getNecessaryValue(Key.ES_HOST,ESReaderErrorCode.BAD_CONFIG_VALUE);
    String port = this.readerOriginConfig.getNecessaryValue(Key.ES_PORT,ESReaderErrorCode.BAD_CONFIG_VALUE);
    this.client = ESClient.getClient(host, port);

    LOG.info("validate() end... ");

  }

/**
 *
 * @author Stone
 * @date 2018/4/10 15:52
 * @return void
 * @desc  ESReader暂不支持切分，直接返回原配置文件
 *
 */
  public List<Configuration> split() {

    LOG.info("split() begin...");
    LOG.info("ESReader 不支持切分操作");
    List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();
    readerSplitConfigs.add(this.readerOriginConfig);

    LOG.info("split() end...");

    return readerSplitConfigs;
  }

  public void destroy() {
    this.client.close();
  }

}
