package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.elasticsearchreader.util.ColumnEntry;
import com.alibaba.datax.plugin.reader.elasticsearchreader.util.ESClient;
import com.alibaba.druid.pool.ElasticSearchResultSet;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.Token;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.jdbc.ObjectResult;
import org.nlpcn.es4sql.jdbc.ObjectResultsExtractException;
import org.nlpcn.es4sql.jdbc.ObjectResultsExtractor;
import org.nlpcn.es4sql.parse.ElasticSqlExprParser;
import org.nlpcn.es4sql.parse.SqlParser;
import org.nlpcn.es4sql.query.QueryAction;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class ESReaderTask{

private static final Logger LOG = LoggerFactory.getLogger(ESReaderTask.class);

  private Configuration readerSliceConfig;
  private TransportClient client;
  private SearchDao searchDao;

  public void init(Configuration readerSliceConfig) {

    this.readerSliceConfig = readerSliceConfig;

    this.validateColumn(this.readerSliceConfig);

    String host = this.readerSliceConfig.getString(Key.ES_HOST);
    String port = this.readerSliceConfig.getString(Key.ES_PORT);

    this.client = ESClient.getClient(host,port);
    this.searchDao = new SearchDao(client);
  }

  public void startRead(RecordSender recordSender,TaskPluginCollector taskPluginCollector) {

    // 查询语句
    String sql = this.readerSliceConfig.getString(Key.QUERY_SQL);

    SQLQueryExpr sqlExpr = (SQLQueryExpr) toSqlExpr(sql);

    try {

      Select select = new SqlParser().parseSelect(sqlExpr);

      QueryAction queryAction = searchDao.explain(sql);

      SqlElasticSearchRequestBuilder sqlElasticSearchRequestBuilder = (SqlElasticSearchRequestBuilder) queryAction.explain();

      SearchResponse scrollResp = (SearchResponse) sqlElasticSearchRequestBuilder.get();

      int timeoutInMilli = getTimeout(select);

      if (timeoutInMilli != -1){
        //Scroll until no hits are returned
        do {
//          for (SearchHit hit : scrollResp.getHits().getHits()) {
//            //Handle the hit...
//            handleHit(hit, select, recordSender);
//          }
          handleHits(scrollResp.getHits(), recordSender, taskPluginCollector);

          scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(timeoutInMilli)).execute().actionGet();

          // Zero hits mark the end of the scroll and the while loop.
        } while(scrollResp.getHits().getHits().length != 0);

      }else{
        handleHits(scrollResp.getHits(), recordSender, taskPluginCollector);
      }

    } catch (SqlParseException e) {

      e.printStackTrace();

    } catch (SQLFeatureNotSupportedException e) {

      e.printStackTrace();

    }
  }


  private static SQLExpr toSqlExpr(String sql) {

    SQLExprParser parser = new ElasticSqlExprParser(sql);
    SQLExpr expr = parser.expr();

    if (parser.getLexer().token() != Token.EOF) {
      throw new ParserException("illegal sql expr : " + sql);
    }
    return expr;
  }

  /**
   *
   * @author Stone
   * @date 2018/4/12 15:33
   * @param [select]
   * @return int
   * @desc Scroll时获取之前配置的timeout
   *
   */
  private int getTimeout(Select select) {
      Hint scrollHint = null;
      int timeoutInMilli = -1;
      for (Hint hint : select.getHints()) {
        if (hint.getType() == HintType.USE_SCROLL) {
          scrollHint = hint;
          break;
        }
      }
      if (scrollHint != null) {
  //      int scrollSize = (Integer) scrollHint.getParams()[0];
        timeoutInMilli = (Integer) scrollHint.getParams()[1];
      }

      return timeoutInMilli;
    }

  private void handleHits(Object hit, RecordSender recordSender, TaskPluginCollector taskPluginCollector){

    //获取column配置项
    List<ColumnEntry> columnConfigs = getListColumnEntry(readerSliceConfig, Key.COLUMN);

    ResultSet resultSet = getResultSet(hit);

    handleRecord(recordSender, columnConfigs, resultSet ,taskPluginCollector);

  }

   private enum Type {
    STRING, INT, LONG, BOOLEAN, DOUBLE, DATE
  }

  /**
   *
   * @author Stone
   * @date 2018/4/12 15:32
   * @param [searchHits]
   * @return java.sql.ResultSet
   * @desc 将es接口获取到的数据转换成 RS
   *
   */
  private ResultSet getResultSet(Object searchHits) {

    ResultSet rs = null;

    try {

      ObjectResult extractor = new ObjectResultsExtractor(false, false, false).extractResults(searchHits, false);

      List<String> headers = extractor.getHeaders();
      List<List<Object>> lines = extractor.getLines();

      rs = new ElasticSearchResultSet(null, headers, lines);

      } catch (ObjectResultsExtractException e) {
        e.printStackTrace();
      }
    return rs;
  }
  /**
   *
   * @author Stone
   * @date 2018/4/12 15:28
   * @param [\recordSender, columnConfigs, resultSet, taskPluginCollector]
   * @return void
   * @desc 对数据进行抽取转换为Record，发送到writer端
   *
   */
  private void handleRecord(RecordSender recordSender, List<ColumnEntry> columnConfigs, ResultSet resultSet , TaskPluginCollector taskPluginCollector) {

    Column columnGenerated = null;
    Record record = null;
    try {
      while (resultSet.next()) {

        try {
          record = recordSender.createRecord();

          for (ColumnEntry columnConfig : columnConfigs) {

            String columnName = columnConfig.getName();
            String columnType = columnConfig.getType();

            String columnValue = null;
            Type type = Type.valueOf(columnType.toUpperCase());

            try{
              Object value = resultSet.getObject(columnName);
              columnValue = String.valueOf(value);
            } catch (Exception e){
              //获取不到，当null处理
            }

            switch (type) {
              case STRING:
                columnGenerated = new StringColumn(columnValue);
                break;
              case INT:
              case LONG:
                try {
                  columnGenerated = new LongColumn(Long.valueOf(columnValue));
                } catch (Exception e) {
                  throw new IllegalArgumentException(String.format(
                      "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                      "LONG"));
                }
                break;
              case DOUBLE:
                try {
                  columnGenerated = new DoubleColumn(Double.valueOf(columnValue));
                } catch (Exception e) {
                  throw new IllegalArgumentException(String.format(
                      "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                      "DOUBLE"));
                }
                break;
              case BOOLEAN:
                try {
                  columnGenerated = new BoolColumn(Boolean.valueOf(columnValue));
                } catch (Exception e) {
                  throw new IllegalArgumentException(String.format(
                      "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                      "BOOLEAN"));
                }
                break;
              case DATE:
                try {
                  if (columnValue == null) {
                    Date date = null;
                    columnGenerated = new DateColumn(date);
                  } else {
                    String formatString = columnConfig.getFormat();
                    //if (null != formatString) {
                    if (StringUtils.isNotBlank(formatString)) {
                      // 建议配置format
                      // 用户自己配置的格式转换, 脏数据行为出现变化
                      DateFormat format = columnConfig
                          .getDateFormat();
                      columnGenerated = new DateColumn(
                          format.parse(columnValue));
                    } else {
                      // 框架尝试转换
                      columnGenerated = new DateColumn(
                          new StringColumn(columnValue)
                              .asDate());
                    }
                  }
                } catch (Exception e) {
                  throw new IllegalArgumentException(String.format(
                      "类型转换错误, 无法将[%s] 转换为[%s]", resultSet.getDate(columnName),
                      "DATE"));
                }
                break;
              default:
                String errorMessage = String.format(
                    "您配置的列类型暂不支持 : [%s]", columnType);
                LOG.error(errorMessage);
                throw DataXException
                    .asDataXException(
                        ESReaderErrorCode.NOT_SUPPORT_TYPE,
                        errorMessage);
            }
            record.addColumn(columnGenerated);
          }
          recordSender.sendToWriter(record);

        }catch (Exception e){

          // 每一种转换失败都是脏数据处理,包括数字格式 & 日期格式
          taskPluginCollector.collectDirtyRecord(record, e.getMessage());
        }
      }

    }  catch(Exception e){
      if (e instanceof DataXException) {
        throw (DataXException) e;
      }
    }
  }

  /**
   *
   * @author Stone
   * @date 2018/4/12 15:30
   * @param [readerConfiguration]
   * @return void
   * @desc 验证配置的column列，现阶段抽取的字段以配置的column列为准，与具体抽取到的数据(即：RS)不相干
   *
   */
  private void validateColumn(Configuration readerConfiguration) {

    List<Configuration> columns = readerConfiguration
        .getListConfiguration(Key.COLUMN);
    if (null == columns || columns.size() == 0) {
      throw DataXException.asDataXException(ESReaderErrorCode.REQUIRED_VALUE, "您需要指定 columns");
    }
    // handle ["*"] 暂不支持设置 ["*]
//    if (1 == columns.size()) {
//      String columnsInStr = columns.get(0).toString();
//      if ("\"*\"".equals(columnsInStr) || "'*'".equals(columnsInStr)) {
//        readerConfiguration.set(Key.COLUMN, null);
//        columns = null;
//      }
//    }
    for (Configuration eachColumnConf : columns) {
      eachColumnConf.getNecessaryValue(Key.TYPE, ESReaderErrorCode.REQUIRED_VALUE);
      String columnName = eachColumnConf.getString(Key.NAME);

      if (null == columnName) {
        throw DataXException.asDataXException(ESReaderErrorCode.REQUIRED_VALUE,"由于您配置了type, 则需要配置 name ");
      }
    }
  }


  public static List<ColumnEntry> getListColumnEntry(Configuration configuration, final String path) {
    List<JSONObject> lists = configuration.getList(path, JSONObject.class);
    if (lists == null) {
      return null;
    }
    List<ColumnEntry> result = new ArrayList<ColumnEntry>();
    for (final JSONObject object : lists) {
      result.add(JSON.parseObject(object.toJSONString(),
          ColumnEntry.class));
    }
    return result;
  }



  public void destroy() {
    this.client.close();
  }



}
