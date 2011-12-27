package com.solab.sql

import java.sql.Connection
import javax.sql.DataSource

/** Keeps track of the connection status for a given thread.
 *
 * @author Enrique Zamudio
 * Date: 23/12/11 14:56
 */
class ConnectionStatus(val ds:DataSource) {

  private var tx=false
  private var conn:Connection=_

  /** Return the current connection, or create a new one if it doesn't exist. */
  def connection()={
    if (conn == null || conn.isClosed) {
      conn = ds.getConnection
    }
    conn
  }

  /** Close the connection, unless there's a transaction in progress. */
  def close() {
    if (!tx) {
      conn.close()
    }
  }

  /** Sets the transaction flag, and the connection's autocommit property to false. */
  def beginTransaction() {
    tx = true
    connection().setAutoCommit(false)
  }
  /** Commits the current transaction and clears the transaction flag. If an exception is thrown during
   * commit, the transaction is automatically rolled back and the exception is rethrown. */
  def commit() {
    try {
      connection().commit()
    } catch {
      case ex:Throwable =>
        connection().rollback()
        throw ex
    } finally {
      tx = false
    }
  }
  /** Clears the transaction flag and rolls back the current transaction. */
  def rollback() {
    tx = false
    connection().rollback()
  }

}

/** The Sql object uses this object to manage thread-safe connections.
 *
 * @author Enrique Zamudio
 */
class ThreadLocalConnection(ds:DataSource) extends ThreadLocal[ConnectionStatus] {
  override def initialValue=new ConnectionStatus(ds)
}
