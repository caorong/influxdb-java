package org.influxdb;

import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBService;
import org.junit.Test;

import com.github.kevinsawicki.http.HttpRequest;

/**
 * desc...
 *
 * @author caorong
 */
public class InfluxDbServiceTest {

  InfluxDBService influxDBService = new InfluxDBService("http://localhost:8086");

  @Test
  public void testping(){
    HttpRequest httpRequest = influxDBService.ping();
    System.out.println(httpRequest.code());
    System.out.println(httpRequest.body());
  }

  InfluxDB influxDB = InfluxDBFactory.connect("http://" + TestUtils.getInfluxIP() + ":8086", "root", "root");
  @Test
  public void ping2() throws InterruptedException {
    boolean influxDBstarted = false;
    do {
      Pong response;
      try {
        response = influxDB.ping();
        System.out.println(response);
        if (!response.getVersion().equalsIgnoreCase("unknown")) {
          influxDBstarted = true;
        }
      } catch (Exception e) {
        // NOOP intentional
        e.printStackTrace();
      }
      Thread.sleep(100L);
    } while (!influxDBstarted);
  }

  @Test
  public void queryTest() { //CREATE DATABASE mydb2
    QueryResult queryResult= influxDBService.query("root", "root", "mydb", "CREATE DATABASE mydb2");
    System.out.println(queryResult);
  }

  @Test
  public void writeTest(){
    BatchPoints batchPoints = BatchPoints.database("mydb3").tag("async", "true").retentionPolicy("default").build();
    Point point1 = Point
        .measurement("cpu")
        .tag("atag", "test")
        .addField("idle", 90L)
        .addField("usertime", 9L)
        .addField("system", 1L)
        .build();
    Point point2 = Point.measurement("disk").tag("atag", "test").addField("used", 80L).addField("free", 1L).build();
    batchPoints.point(point1);
    batchPoints.point(point2);
    HttpRequest httpRequest = influxDBService.writePoints("root", "root", "mydb3", "default", "n", "any",
        batchPoints.lineProtocol());
    System.out.println(httpRequest.code());
    System.out.println(httpRequest.body());
  }
}
