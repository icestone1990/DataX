package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

public class ESReader extends Reader {

  public static class Job extends Reader.Job{

    private static final Logger LOG = LoggerFactory.getLogger(Job.class);

    private Configuration originalConfig;
    private ESReaderJob esReaderJob;

    @Override
    public void init() {
      this.originalConfig = super.getPluginJobConf();
      this.esReaderJob = new ESReaderJob();
      this.esReaderJob.init(this.originalConfig);
    }

    @Override
    public List<Configuration> split(int adviceNumber) {
      return this.esReaderJob.split();
    }

    @Override
    public void destroy() {
      this.esReaderJob.destroy();
    }

  }

  public static class Task extends Reader.Task{

    private Configuration readerSliceConfig;
    private ESReaderTask esReaderTask;

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    @Override
    public void init() {
      this.readerSliceConfig = super.getPluginJobConf();
      this.esReaderTask = new ESReaderTask();
      this.esReaderTask.init(this.readerSliceConfig);
    }

    @Override
    public void startRead(RecordSender recordSender) {
      TaskPluginCollector taskPluginCollector = super.getTaskPluginCollector();
      this.esReaderTask.startRead(recordSender, taskPluginCollector);
    }

    @Override
    public void destroy() {
      this.esReaderTask.destroy();
    }
  }
}
