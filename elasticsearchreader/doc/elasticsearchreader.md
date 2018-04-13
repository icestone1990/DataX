# DataX ElasticSearchReader


---

## 1 快速介绍

elasticsearch数据导出的插件

## 2 实现原理

使用elasticsearch java的接口， 批量从elasticsearch读取数据

## 3 功能说明

### 3.1 配置样例

#### job.json

```
{
  "job": {
    "setting": {
        "speed": {
            "channel": 1
        }
    },
    "content": [
      {
        "writer": {
          ...
        },
        "reader": {
          "name": "elasticsearchreader",
          "parameter": {
            "es_host": "xxx",
            "es_port": "9200"
            "querySql": "select x,xx,xxx from xxxx",
            "column": [
               {
                "name": "name",
                "type": "string"
               },
               {
                "name": "age",
                "type": "int"
               },
               {
                "name": "gender",
                "type": "boolean"
               }
            ]
          }
        }
      }
    ]
  }
}
```

#### 3.2 参数说明

* es_host
 * 描述：ElasticSearch的连接地址
 * 必选：是
 * 默认值：无

* es_host
 * 描述：http auth中的user
 * 必选：是
 * 默认值：无

* column
 * 描述：抽取的字段名称和类型
 * 必选：是

* querySql
 * 描述：elasticsearch-sql语句支持
 * 必选：是

** 从es中获取到的字段由querySql决定，是否抽取传送到writer端由column控制