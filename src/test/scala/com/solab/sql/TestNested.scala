package com.solab.sql

import org.specs2.SpecificationWithJUnit
import org.specs2.specification.Step
import org.apache.commons.dbcp.BasicDataSource
import java.sql.Connection
import java.util.Date

/** Test nested queries and operations.
 *
 * @author Enrique Zamudio
 * Date: 19/01/12 13:40
 */

class TestNested extends SpecificationWithJUnit { def is =

  "Test nested queries and operations" ^ Step(setup()) ^
  "Master-detail" ! masterDetail ^
  "Query + inserts" ! queryAndInserts ^
  "Query + updates" ! queryAndUpdates ^
  "Query + deletes" ! queryAndDeletes ^
  Step(shutdown()) ^
  end

  var ds:BasicDataSource=_
  lazy val sql = new Sql(ds)

  def setup() {
    //Create a pooled datasource for a test database
    ds = new BasicDataSource
    ds.setDriverClassName("org.h2.Driver")
    ds.setUrl("jdbc:h2:mem:scala_sql_tests;MODE=PostgreSQL")
    ds.setDefaultAutoCommit(true)
    ds.setUsername("sa")
    ds.setPassword("")
    ds.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    ds.setInitialSize(Runtime.getRuntime.availableProcessors)
    //Now create some tables
    sql.execute("""
    CREATE TABLE nqmaster(
      id SERIAL PRIMARY KEY,
      name VARCHAR(80),
      date TIMESTAMP
    )""")
    sql.execute("""
    CREATE TABLE nqdetail(
      did SERIAL PRIMARY KEY,
      master INTEGER,
      sub VARCHAR(80),
      price DECIMAL
    )""")
  }
  def shutdown() {
    ds.close()
  }

  def masterDetail()={
    //Insert some data
    1 to 10 foreach { a =>
      val key = sql.executeInsert("INSERT INTO nqmaster(name, date) VALUES (?, ?)", "master_t1_"+a, new Date)(0)(0)
      1 to 5 foreach { b =>
        sql.executeInsert("INSERT INTO nqdetail(master, sub, price) VALUES (?, ?, ?)", key, "sub_t1 %d-%d".format(key, b), b*1.25)
      }
    }
    //Now the test of nested queries
    var count = 0
    sql.eachRow("SELECT * FROM nqmaster WHERE name LIKE ?", "master_t1_%") { master =>
      sql.eachRow("SELECT * FROM nqdetail WHERE master=?", master("id")) { detail =>
        count += 1
      }
    }
    count must be equalTo 50
  }

  def queryAndInserts()={
    //Insert some data
    1 to 10 foreach { a =>
      sql.executeInsert("INSERT INTO nqmaster(name, date) VALUES (?, ?)", "master_t2_"+a, new Date)(0)(0)
    }
    //Now test a query and nested inserts
    sql.eachRow("SELECT * FROM nqmaster WHERE name LIKE ?", "master_t2_%") { master =>
      sql.execute("INSERT INTO nqdetail (master, sub, price) VALUES (?, ?, ?)", master("id"), "sub_t2", 0.1)
    }
    sql.queryForInt("SELECT count(*) FROM nqdetail WHERE sub=?", "sub_t2") must be equalTo Some(10)
  }
  def queryAndUpdates()={
    //Insert some data
    1 to 10 foreach { a =>
      val key = sql.executeInsert("INSERT INTO nqmaster(name, date) VALUES (?, ?)", "master_t3_"+a, new Date)(0)(0)
      1 to 5 foreach { b =>
        sql.executeInsert("INSERT INTO nqdetail(master, sub, price) VALUES (?, ?, ?)", key, "sub_t3 %d-%d".format(key, b), b*1.25)
      }
    }
    //Now test nested update
    var count = 0
    sql.eachRow("SELECT * FROM nqmaster WHERE name LIKE ?", "master_t3_%") { master =>
      count += sql.executeUpdate("UPDATE nqdetail SET sub=? WHERE master=?", "updated_sub_t3", master("id"))
    }
    count must be equalTo 50
  }
  def queryAndDeletes()={
    //Insert some data
    1 to 10 foreach { a =>
      val key = sql.executeInsert("INSERT INTO nqmaster(name, date) VALUES (?, ?)", "master_t4_"+a, new Date)(0)(0)
      1 to 5 foreach { b =>
        sql.executeInsert("INSERT INTO nqdetail(master, sub, price) VALUES (?, ?, ?)", key, "sub_t4 %d-%d".format(key, b), b*1.25)
      }
    }
    //Now test nested update
    var count = 0
    sql.eachRow("SELECT * FROM nqmaster WHERE name LIKE ?", "master_t4_%") { master =>
      count += sql.executeUpdate("DELETE FROM nqdetail WHERE master=?", master("id"))
    }
    count must be equalTo 50
  }

}
