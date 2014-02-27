/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.steps.jdbcmetadata;


import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.lang.reflect.Method;
import java.sql.Connection;

import java.util.Map;
import java.util.HashMap;

/**
 * This class is part of the demo step plug-in implementation.
 * It demonstrates the basics of developing a plug-in step for PDI. 
 * 
 * The demo step adds a new string field to the row stream and sets its
 * value to "Hello World!". The user may select the name of the new field.
 *   
 * This class is the implementation of StepDataInterface.
 *   
 * Implementing classes inherit from BaseStepData, which implements the entire
 * interface completely. 
 * 
 * In addition classes implementing this interface usually keep track of
 * per-thread resources during step execution. Typical examples are:
 * result sets, temporary data, caching indexes, etc.
 *   
 * The implementation for the demo step stores the output row structure in 
 * the data class. 
 *   
 */
public class JdbcMetaDataData extends BaseStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;
  //used to store the named kettle connection
  public Database database;
  //used to store the actual jdbc connection
  public Connection connection;
  //used to store the DatabaseMetaData method that generates the data
  public Method method;
  //use to store the arguments to the method
  public Object[] arguments;
  //named kettle connection cache. Used when connection is a named connection specified by input fields
  public Map<String, Database> databases = null;
  //connection cache. Used when connection is a jdbc connection specified by input fields
  public Map<String[], Connection> connections = null;
  //key to the connection cache.
  public String[] connectionKey = null;
  //field index for named kettle connection
  public int connectionField = -1;
  //field indices for jdbc connections
  public int jdbcDriverField = -1;
  public int jdbcUrlField = -1;
  public int jdbcUserField = -1;
  public int jdbcPasswordField = -1;
  //indices for fields used to specify method arguments
  public int[] argumentFieldIndices;
  //the offset in the output row from where we can add our metadata fields.
  //(we need this in case we're required to remove arguments fields from the input)
  public int outputRowOffset = -1;
  //
  public int[] inputFieldsToCopy;
  //the indices of the columns in the resultset
  public int[] resultSetIndices;
  
  public JdbcMetaDataData() {
    super();
  }
  
}

