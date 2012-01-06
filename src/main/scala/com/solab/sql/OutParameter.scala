/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.solab.sql

import java.sql.Types

/** Represents a parameter that is returned from a callable statement.
 *
 * @author Enrique Zamudio
 * Date: 06/01/12 14:06
 */
class OutParameter(val sqlType:Int)

/** Contains references to all OutParameter types.
 *
 * @author Enrique Zamudio
 */
object OutParams {

  val ARRAY         = new OutParameter(Types.ARRAY)
  val BIGINT        = new OutParameter(Types.BIGINT)
  val BINARY        = new OutParameter(Types.BINARY)
  val BIT           = new OutParameter(Types.BIT)
  val BLOB          = new OutParameter(Types.BLOB)
  val BOOLEAN       = new OutParameter(Types.BOOLEAN)
  val CHAR          = new OutParameter(Types.CHAR)
  val CLOB          = new OutParameter(Types.CLOB)
  val DATALINK      = new OutParameter(Types.DATALINK)
  val DATE          = new OutParameter(Types.DATE)
  val DECIMAL       = new OutParameter(Types.DECIMAL)
  val DISTINCT      = new OutParameter(Types.DISTINCT)
  val DOUBLE        = new OutParameter(Types.DOUBLE)
  val FLOAT         = new OutParameter(Types.FLOAT)
  val INTEGER       = new OutParameter(Types.INTEGER)
  val JAVA_OBJECT   = new OutParameter(Types.JAVA_OBJECT)
  val LONGVARBINARY = new OutParameter(Types.LONGVARBINARY)
  val LONGVARCHAR   = new OutParameter(Types.LONGVARCHAR)
  val NULL          = new OutParameter(Types.NULL)
  val NUMERIC       = new OutParameter(Types.NUMERIC)
  val OTHER         = new OutParameter(Types.OTHER)
  val REAL          = new OutParameter(Types.REAL)
  val REF           = new OutParameter(Types.REF)
  val SMALLINT      = new OutParameter(Types.SMALLINT)
  val STRUCT        = new OutParameter(Types.STRUCT)
  val TIME          = new OutParameter(Types.TIME)
  val TIMESTAMP     = new OutParameter(Types.TIMESTAMP)
  val TINYINT       = new OutParameter(Types.TINYINT)
  val VARBINARY     = new OutParameter(Types.VARBINARY)
  val VARCHAR       = new OutParameter(Types.VARCHAR)

}

/** Bidirectional parameters, for statements.
 *
 * @author Enrique Zamudio
 */
class InOutParameter(override val sqlType:Int, val value:Any) extends OutParameter(sqlType:Int)
