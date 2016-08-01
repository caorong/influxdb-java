package org.influxdb.impl;

import java.util.HashMap;
import java.util.Map;

import org.influxdb.dto.QueryResult;

import com.alibaba.fastjson.JSON;
import com.github.kevinsawicki.http.HttpRequest;

/**
 * InfluxDBService impl with HttpRequest(HttpURLConnection)
 *
 * @author caorong
 */
public class InfluxDBService {

  public static final String U = "u";
  public static final String P = "p";
  public static final String Q = "q";
  public static final String DB = "db";
  public static final String RP = "rp";
  public static final String PRECISION = "precision";
  public static final String CONSISTENCY = "consistency";
  public static final String EPOCH = "epoch";

  private String url;
  private String pingURL;
  private String writeURL;
  private String queryURL;

  public InfluxDBService(String url) {
    this.url = url;
    this.pingURL = url + "/ping";
    this.writeURL = url + "/write";
    this.queryURL = url + "/query";
  }

  public HttpRequest ping() {
    return HttpRequest.get(pingURL);
  }

  public HttpRequest writePoints(String username, String password, String database, String retentionPolicy,
      String precision, String consistency, String batchPoints) {
    Map<String, String> queryMap = new HashMap<String, String>(6);
    queryMap.put(U, username);
    queryMap.put(P, password);
    queryMap.put(DB, database);
    queryMap.put(RP, retentionPolicy);
    queryMap.put(PRECISION, precision);
    queryMap.put(CONSISTENCY, consistency);
    return HttpRequest.post(writeURL, queryMap, true).contentType("text/plain").send(batchPoints);
  }

  public QueryResult query(String username, String password, String db, String epoch, String query) {
    Map<String, String> queryMap = new HashMap<String, String>(5);
    queryMap.put(U, username);
    queryMap.put(P, password);
    queryMap.put(DB, db);
    queryMap.put(EPOCH, epoch);
    queryMap.put(Q, query);
    String json = HttpRequest.get(queryURL, queryMap, true).body();
    return JSON.parseObject(json, QueryResult.class);
  }

  public QueryResult query(String username, String password, String db, String query) {
    Map<String, String> queryMap = new HashMap<String, String>(4);
    queryMap.put(U, username);
    queryMap.put(P, password);
    queryMap.put(DB, db);
    queryMap.put(Q, query);
    String json = HttpRequest.get(queryURL, queryMap, true).body();
    return JSON.parseObject(json, QueryResult.class);
  }

  public QueryResult query(String username, String password, String query) {
    Map<String, String> queryMap = new HashMap<String, String>(3);
    queryMap.put(U, username);
    queryMap.put(P, password);
    queryMap.put(Q, query);
    String json = HttpRequest.get(queryURL, queryMap, true).body();
    return JSON.parseObject(json, QueryResult.class);
  }
}
