package com.alibaba.datax.plugin.writer.postgresqlwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.*;

public class PostgresqlWriterTask extends CommonRdbmsWriter.Task {
	private static final Logger LOG = LoggerFactory.getLogger(PostgresqlWriterTask.class);
	private Configuration writerSliceConfig = null;
	private int numProcessor;
	private int numWriter;
	private int queueSize;
	private volatile boolean stopProcessor = false;
	private volatile boolean stopWriter = false;

	private CompletionService<Long> cs = null;
	private ExecutorService threadPool;

	public PostgresqlWriterTask() {
		super(DataBaseType.PostgreSQL);
	}

	public String getJdbcUrl() {
		return this.jdbcUrl;
	}

	public Connection createConnection() {
		Connection connection = DBUtil.getConnection(this.dataBaseType, this.jdbcUrl, username, password);
		DBUtil.dealWithSessionConfig(connection, writerSliceConfig, this.dataBaseType, BASIC_MESSAGE);
		return connection;

	}

	public String getCopySql(String tableName, List<String> columnList, int segment_reject_limit) {
		StringBuilder sb = new StringBuilder().append("COPY ").append(tableName).append("(")
				.append(StringUtils.join(columnList, ","))
				.append(") FROM STDIN WITH DELIMITER '|' NULL '' CSV QUOTE '\"' ESCAPE E'\\\\'");

		if (segment_reject_limit >= 2) {
			sb.append(" LOG ERRORS SEGMENT REJECT LIMIT ").append(segment_reject_limit).append(";");
		} else {
			sb.append(";");
		}

		String sql = sb.toString();
		return sql;
	}

	private void send(Record record, LinkedBlockingQueue<Record> queue)
			throws InterruptedException, ExecutionException {
		while (queue.offer(record) == false) {
			LOG.debug("Record queue is full, increase num_copy_processor for performance.");
			Future<Long> result = cs.poll();

			if (result != null) {
				result.get();
			}

			Thread.sleep(100);
		}
	}

	public boolean moreRecord() {
		return !stopProcessor;
	}

	public boolean moreData() {
		return !stopWriter;
	}

	@Override
	public void startWrite(RecordReceiver recordReceiver, Configuration writerSliceConfig,
			TaskPluginCollector taskPluginCollector) {
		this.writerSliceConfig = writerSliceConfig;

		// 每个计算节点可接受的错误行数，0为不接受，或者大于1的正整数
//		int segment_reject_limit = writerSliceConfig.getInt("segment_reject_limit", 0);

		// 线程异步队列大小，增大此参数增加内存消耗，提升性能
		this.queueSize = writerSliceConfig.getInt("copy_queue_size", 100000);
		this.queueSize = this.queueSize < 1000 ? 1000 : this.queueSize;

		// 格式化数据的线程数
		this.numProcessor = writerSliceConfig.getInt("num_copy_processor", 4);
		this.numProcessor = this.numProcessor < 1 ? 1 : this.numProcessor;

		// 写入数据库的并发数
		this.numWriter = writerSliceConfig.getInt("num_copy_writer", 1);
		this.numWriter = this.numWriter < 1 ? 1 : this.numWriter;

		// 获取copy语句的sql模板
		String sql = getCopySql(this.table, this.columns, 0);

		// inner record
		LinkedBlockingQueue<Record> recordQueue = new LinkedBlockingQueue<Record>(queueSize);

		// record transformer byte[]
		LinkedBlockingQueue<byte[]> dataQueue = new LinkedBlockingQueue<byte[]>(queueSize);


		// 线程池
//		ExecutorService threadPool;
		threadPool = Executors.newFixedThreadPool(this.numProcessor + this.numWriter);
		cs = new ExecutorCompletionService<Long>(threadPool);


		Connection connection = createConnection();

		try {

			this.resultSetMetaData = DBUtil.getColumnMetaData(connection, this.table,
					StringUtils.join(this.columns, ","));
			for (int i = 0; i < numProcessor; i++) {
				cs.submit(new PostgresqlWriterProcessor(this, this.columnNumber, resultSetMetaData, recordQueue, dataQueue));
			}

			for (int i = 0; i < numWriter; i++) {
				cs.submit(new PostgresqlWriterWorker(this, sql, dataQueue));
			}

			Record record;
			while ((record = recordReceiver.getFromReader()) != null) {
				send(record, recordQueue);
				Future<Long> result = cs.poll();

				if (result != null) {
					result.get();
				}
			}

			stopProcessor = true;
			for (int i = 0; i < numProcessor; i++) {
				cs.take().get();
			}

			stopWriter = true;
			for (int i = 0; i < numWriter; i++) {
				cs.take().get();
			}

		} catch (Exception e) {
			throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
		} finally {
			threadPool.shutdownNow();
			DBUtil.closeDBResources(null, null, connection);
		}
	}
}
