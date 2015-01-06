/*******************************************************************************
 *
 * JdbcMetaData plugin step for Pentaho Data Integration
 *
 * Copyright (C) 2014 by Roland Bouman: roland.bouman@gmail.com
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
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * This class is part of the demo step plug-in implementation.
 * It demonstrates the basics of developing a plug-in step for PDI. 
 * 
 * The demo step adds a new string field to the row stream and sets its
 * value to "Hello World!". The user may select the name of the new field.
 *   
 * This class is the implementation of StepInterface.
 * Classes implementing this interface need to:
 * 
 * - initialize the step
 * - execute the row processing logic
 * - dispose of the step 
 * 
 * Please do not create any local fields in a StepInterface class. Store any
 * information related to the processing logic in the supplied step data interface
 * instead.  
 * 
 */

public class JdbcMetaData extends BaseStep implements StepInterface {

  /**
   * The constructor should simply pass on its arguments to the parent class.
   * 
   * @param s step description
   * @param stepDataInterface	step data class
   * @param c step copy
   * @param t transformation description
   * @param dis transformation executing
   */
  public JdbcMetaData(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis) {
    super(s, stepDataInterface, c, t, dis);
  }

  /**
   * Utility to create a jdbc connection. (Used when the connection source is JDBC or JDBCFields)
   * @param driver The fully qualified classname identifying the jdbc driver
   * @param url The driver-specific url to create the connection
   * @param user The database user that wants to establish a connection
   * @param password The password required to establish the connection
   * @return A jdbc connection object.
   * @throws Exception
   */
  private Connection createJdbcConnection(String driver, String url, String user, String password) throws Exception{
    Class.forName(driver);
    return DriverManager.getConnection(url, user, password);
  }

  /**
   * Utility to convert a string value entered by the user into an argument (or argument array element) for a metadata method.
   * @param stringValue string value
   * @param type class that represents the type of the value as it is passed as argument to a metadata method
   * @return an object that represents the value in the specified type
   * @throws IllegalArgumentException
   */
  private Object stringToArgumentValue(String stringValue, Class<?> type) throws IllegalArgumentException{
    Object argument;
    if (type == String.class) {
      argument = (String)stringValue;
    }
    else 
    if (type == Boolean.class) {
      argument = "Y".equals(stringValue) ? Boolean.TRUE : Boolean.FALSE;
    }
    else
    if (type == Integer.class){
      argument = new Integer(stringValue);
    }
    else {
      throw new IllegalArgumentException("Can't handle valueType " + type.getName());
    }
    return argument;
  }
  /**
   * Some method arguments are passed as arrays. 
   * In Kettle, users represent this as a comma-separated list of values
   * This method converts from the user value to the actual argument
   * @return
   */
  private Object stringListToObjectArray(String stringValue, Class<?> type){
    if (stringValue == null) return null;
    String[] stringValues = stringValue.split(",");
    int n = stringValues.length;
    Object result = Array.newInstance(type, n);
    for (int i = 0; i < n; i++) {
      Array.set(result, i, stringToArgumentValue(stringValues[i], type));
    }
    return result;
  }
  
  /**
   * Utility to get the kettle named database connection
   * @param connectionName The name of the kettle connection
   * @return The database object
   */
  private Database findDatabase(String connectionName){
    DatabaseMeta databaseMeta = getTransMeta().findDatabase(connectionName);
    if (databaseMeta == null) {
      String message = "Named connection " + connectionName + " not found!";
      throw new IllegalArgumentException(message);
    }
    Database database = new Database(databaseMeta);
    return database;
  }

  /**
   * Utility to set up the method and its arguments
   * @param meta
   * @param data
   * @throws Exception
   */
  private void initMethod(JdbcMetaDataMeta meta, JdbcMetaDataData data) throws Exception {
    //set up the method to call
    logDebug("Setting up method to call.");
    Method method = meta.getMethod();
    data.method = method;

    //Try to set up the arguments for the method
    logDebug("Setting up method arguments.");
    Class<?>[] argumentTypes = method.getParameterTypes();
    int argc = argumentTypes.length;
    logDebug("Method has " + argc + " arguments.");
    String[] arguments = meta.getArguments();
    logDebug("We expected " + arguments.length + " arguments.");
    if (argc != arguments.length) {
      throw new Exception("Method has a " + argc + " arguments, we expected " + arguments.length);
    }
    logDebug("Allocating arguments array.");
    data.arguments = new Object[argc];
    String stringArgument;
    if (meta.getArgumentSourceFields()) {
      //arguments are specified by values coming from fields.
      //we don't know about the fields yet, so we 
      //initialize with bogus value so we can check if all fields were found
      logDebug("Allocating fieldindices array for arguments.");
      data.argumentFieldIndices = new int[argc];
      for (int i = 0; i < argc; i++) {
        data.argumentFieldIndices[i] = -1;
      }
    }
    else {
      //arguments are specified directly by the user in the step.
      //here we convert the string values into proper argument values
      Class<?> argumentType;
      Object argument;
      for (int i = 0; i < argc; i++) {
        argumentType = argumentTypes[i];
        stringArgument = arguments[i];
        if (stringArgument == null) {
          argument = null;
        }
        else {
          stringArgument = environmentSubstitute(stringArgument);
          if (argumentType.isArray()) {
            if (stringArgument.length() == 0) {
              argument = null;
            }
            else {
              argument = stringListToObjectArray(stringArgument, argumentType.getComponentType());
            }
          }
          else {
            argument = stringToArgumentValue(stringArgument, argumentType);        
          }
        }
        data.arguments[i] = argument;
      }
    }
  }

  /**
   * Initialize the connection
   * @param meta
   * @param data
   * @throws Exception
   */
  private void initConnection(JdbcMetaDataMeta meta, JdbcMetaDataData data) throws Exception {
    //Try to establish a connection in advance
    logDebug("Try to establish a connection in advance.");
    String connectionSource = meta.getConnectionSource();
    logDebug("Connection source: " + connectionSource);
    Connection connection;
    if (JdbcMetaDataMeta.connectionSourceOptionConnection.equals(connectionSource)) {
      //connection is a named kettle connection
      Database database = findDatabase(meta.getConnectionName());
      database.connect();
      connection = database.getConnection();
      if (connection == null) {
        throw new Exception("Connection returned by database object is null!");
      }
      data.database = database;
    }
    else
    if (JdbcMetaDataMeta.connectionSourceOptionJDBC.equals(connectionSource)) {
      //connection is a user-entered jdbc connection
      String jdbcDriver = environmentSubstitute(meta.getJdbcDriverField());
      String jdbcUrl = environmentSubstitute(meta.getJdbcUrlField());
      String jdbcUser = environmentSubstitute(meta.getJdbcUserField());
      String jdbcPassword = environmentSubstitute(meta.getJdbcPasswordField());
      logDebug("Attempt to create JDBC connection.");
      logDebug("Driver: " + jdbcDriver);
      logDebug("Url: " + jdbcUrl);
      logDebug("User: " + jdbcUser);
      connection = createJdbcConnection(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword);
    }
    else
    if (JdbcMetaDataMeta.connectionSourceOptionJDBCFields.equals(connectionSource)) {
      //Connection is a jdbc connection specified by field values
      //we don't know the field values yet, but we can initialize a few vars to access their value later on
      connection = null;
      data.jdbcDriverField = -1;
      data.jdbcUrlField = -1;
      data.jdbcUserField = -1;
      data.jdbcPasswordField = -1;
    }
    else
    if (JdbcMetaDataMeta.connectionSourceOptionConnectionField.equals(connectionSource)){
      //Connection is a named kettle connection specified by a field value
      //we don't know about the field value yet, but we can initialize a var to access its value later on
      connection = null;
      data.connectionField = -1;
    }
    else {
      //should not arrive here, just initialize the connection to make the compiler shut up.
      connection = null;
    }
    //if the connection is null at this point, then we need to establish it on a row by row basis
    data.connection = connection;
  }

  private void initOutputFields(JdbcMetaDataMeta meta, JdbcMetaDataData data){
    Object[] outputFields = meta.getOutputFields();
    int n = outputFields.length;
    data.resultSetIndices = new int[n];
    
    ValueMetaInterface[] fields = meta.getMethodResultSetDescriptor();
    int m = fields.length;
    
    for (int i = 0; i < n; i++) {
      String[] outputField = (String[])outputFields[i];
      String fieldName = outputField[0];
      
      for (int j = 0; j < m; j++) {
        ValueMetaInterface field = fields[j];
        if (!fieldName.equals(field.getName())) continue;
        data.resultSetIndices[i] = j + 1;
        break;
      }
    }
  }

  /**
   * This method is called by PDI during transformation startup. 
   * 
   * It should initialize required for step execution. 
   * 
   * The meta and data implementations passed in can safely be cast
   * to the step's respective implementations. 
   * 
   * It is mandatory that super.init() is called to ensure correct behavior.
   * 
   * Typical tasks executed here are establishing the connection to a database,
   * as wall as obtaining resources, like file handles.
   * 
   * @param smi 	step meta interface implementation, containing the step settings
   * @param sdi	step data interface implementation, used to store runtime information
   * 
   * @return true if initialization completed successfully, false if there was an error preventing the step from working. 
   *  
   */
  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    boolean result = true;
    // Casting to step-specific implementation classes is safe
    JdbcMetaDataMeta meta = (JdbcMetaDataMeta) smi;
    JdbcMetaDataData data = (JdbcMetaDataData) sdi;
    if (!super.init(meta, data)) return false;
    
    try {
      data.databases = new HashMap<String, Database>();
      data.connections = new HashMap<String[], Connection>();
      data.connectionKey = new String[4];
      initMethod(meta, data);
      initConnection(meta, data);
      initOutputFields(meta, data);
    }
    catch(Exception exception) {
      logError("Unexpected " + exception.getClass().getName() +" initializing step: " + exception.getMessage());
      result = false;
    }
    return result;
  }

  private String getConnectionNameFromRow(JdbcMetaDataData data, Object[] row){
    return (String)(row[data.connectionField]);
  }
  
  private String getJdbcDriverFromRow(JdbcMetaDataData data, Object[] row) {
    return (String)(row[data.jdbcDriverField]);
  }

  private String getJdbcUrlFromRow(JdbcMetaDataData data, Object[] row) {
    return (String)(row[data.jdbcUrlField]);
  }

  private String getJdbcUserFromRow(JdbcMetaDataData data, Object[] row) {
    return (String)(row[data.jdbcUserField]);
  }

  private String getJdbcPasswordFromRow(JdbcMetaDataData data, Object[] row) {
    return (String)(row[data.jdbcPasswordField]);
  }

  /**
   * This is called in the processRow function to obtain the contain to apply the metadat method to.
   * @param meta
   * @param data
   * @param row
   * @return
   * @throws Exception
   */
  private Connection getConnection(JdbcMetaDataMeta meta, JdbcMetaDataData data, Object[] row) throws Exception{
    Connection connection;
    if (data.connection == null) {
      //connection could not be initialized at init, so we have to obtain it now on a row by row basis.
      String connectionSource = meta.getConnectionSource();
      if (JdbcMetaDataMeta.connectionSourceOptionConnectionField.equals(connectionSource)) {
        //connection is a named kettle connection specified by a field value
        String connectionName = getConnectionNameFromRow(data, row);
        //try to get this named connection from the cache
        Database database = data.databases.get(connectionName);
        if (database == null) {
          //we haven't seen this named connection before, try to find it.
          database = findDatabase(connectionName);
          database.connect();
          connection = database.getConnection();
          if (connection == null) {
            throw new IllegalArgumentException("Connection returned by database is null!");
          }
          //cache the database for later use
          data.databases.put(connectionName, database);
        }
        else {
          //database found in cache, get its connection
          connection = database.getConnection();
        }
      }
      else
      if (JdbcMetaDataMeta.connectionSourceOptionJDBCFields.equals(connectionSource)) {
        //database connectin is a jdbc connection defined by field values.
        //try to find this connection in the cache
        String[] key = data.connectionKey;
        key[0] = getJdbcDriverFromRow(data, row);
        key[1] = getJdbcUrlFromRow(data, row);
        key[2] = getJdbcUserFromRow(data, row);
        key[3] = getJdbcPasswordFromRow(data, row);
        connection = data.connections.get(key);
        if (connection == null) {
          //connection not yet in the cache. Let's create it and cahce it.
          connection = createJdbcConnection(key[0], key[1], key[2], key[3]);
          data.connections.put(key, connection);
        }
      }
      else {
        throw new Exception("Unexpected error acquiring connection");
      }
    }
    else {
      connection = data.connection;
    }
    return connection;
  }

  /**
   * This is called in the processRow function to get the actual arguments for the jdbc metadata method
   * @param meta
   * @param data
   * @param row
   * @throws Exception
   */
  private void prepareMethodArguments(JdbcMetaDataMeta meta, JdbcMetaDataData data, Object[] row) throws Exception {
    //if the arguments are not from fields, then we already took care of it in the init phase, so leave
    if (!meta.getArgumentSourceFields()) return;
    //if the arguments are from fields, then we already stored the right indices to take the values from
    Object[] args = data.arguments;
    int[] indices = data.argumentFieldIndices;
    int index;
    Class<?>[] argumentTypes = data.method.getParameterTypes();
    Class<?> argumentType;
    Object argument;
    for (int i = 0; i < args.length; i++){
      index = indices[i];
      if (index == -2) {
        argument = null;
      }
      else {
        argument = row[index];
      }
      argumentType = argumentTypes[i];
      if (argumentType.isArray() && argument != null) {
        if ("".equals(argument)) {
          logDebug("Converted empty string to null for argument array");
          argument = null;
        }
        else {
          argument = stringListToObjectArray((String)argument, argumentType.getComponentType());
        }
      }
      args[i] = argument;
    }
  }

  /**
   * This is called in the processRow function to obtain a output row.
   * @param meta
   * @param data
   * @param inputRow
   * @return
   */
  private Object[] createOutputRow(JdbcMetaDataMeta meta, JdbcMetaDataData data, Object[] inputRow){
    Object[] outputRow = new Object[data.outputRowMeta.size()];
    if (data.inputFieldsToCopy == null) {
      System.arraycopy(inputRow, 0, outputRow, 0, getInputRowMeta().size());
    }
    else {
      for (int i = 0; i < data.inputFieldsToCopy.length; i++) {
        outputRow[i] = inputRow[data.inputFieldsToCopy[i]];
      }
    }
    return outputRow;
  }
  /**
   * Once the transformation starts executing, the processRow() method is called repeatedly
   * by PDI for as long as it returns true. To indicate that a step has finished processing rows
   * this method must call setOutputDone() and return false;
   * 
   * Steps which process incoming rows typically call getRow() to read a single row from the
   * input stream, change or add row content, call putRow() to pass the changed row on 
   * and return true. If getRow() returns null, no more rows are expected to come in, 
   * and the processRow() implementation calls setOutputDone() and returns false to
   * indicate that it is done too.
   * 
   * Steps which generate rows typically construct a new row Object[] using a call to
   * RowDataUtil.allocateRowData(numberOfFields), add row content, and call putRow() to
   * pass the new row on. Above process may happen in a loop to generate multiple rows,
   * at the end of which processRow() would call setOutputDone() and return false;
   * 
   * @param smi the step meta interface containing the step settings
   * @param sdi the step data interface that should be used to store
   * 
   * @return true to indicate that the function should be called again, false if the step is done
   */
  public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

    // safely cast the step settings (meta) and runtime info (data) to specific implementations 
    JdbcMetaDataMeta meta = (JdbcMetaDataMeta) smi;
    JdbcMetaDataData data = (JdbcMetaDataData) sdi;

    // get incoming row, getRow() potentially blocks waiting for more rows, returns null if no more rows expected
    Object[] r = getRow(); 

    // if no more rows are expected, indicate step is finished and processRow() should not be called again
    if (r == null){
      setOutputDone();
      return false;
    }

    // the "first" flag is inherited from the base step implementation
    // it is used to guard some processing tasks, like figuring out field indexes
    // in the row structure that only need to be done once
    if (first) {
      first = false;
      RowMetaInterface inputRowMeta = getInputRowMeta();
      data.outputRowOffset = inputRowMeta.size();
      String connectionSource = meta.getConnectionSource();
      boolean argumentSourceFields = meta.getArgumentSourceFields();
      //check if we need to use fields. 
      //If so, store the indices so we can easily extract their values during transformation
      if (
        JdbcMetaDataMeta.connectionSourceOptionJDBCFields.equals(connectionSource)
      ||JdbcMetaDataMeta.connectionSourceOptionConnectionField.equals(connectionSource)
      ||argumentSourceFields 
      ) {
        logDebug("Looking up indices of input fields.");
        String fieldName;
        String[] fieldNames = inputRowMeta.getFieldNames();
        logDebug("We have " + fieldNames.length + " input fields.");
        String[] arguments = meta.getArguments();
        int argc = arguments.length;
        String stringArgument;
        for (int i = 0; i < fieldNames.length; i++) {
          fieldName = fieldNames[i];
          logDebug("Looking at field: " + fieldName);
          //store indices for connection source fields
          if (JdbcMetaDataMeta.connectionSourceOptionConnectionField.equals(connectionSource)){
            if (fieldName.equals(meta.getConnectionField())) {
              logDebug("Found the connection field at index: " + i);
              data.connectionField = i;
            }
          }
          else
          if (JdbcMetaDataMeta.connectionSourceOptionJDBCFields.equals(connectionSource)) {
            if (fieldName.equals(meta.getJdbcDriverField())) {
              logDebug("Found the jdbcDriverField field at index: " + i);
              data.jdbcDriverField = i;
            }
            if (fieldName.equals(meta.getJdbcUrlField())) {
              logDebug("Found the jdbcUrlField field at index: " + i);
              data.jdbcUrlField = i;
            }
            if (fieldName.equals(meta.getJdbcUserField())) {
              logDebug("Found the jdbcUserField field at index: " + i);
              data.jdbcUserField = i;
            }
            if (fieldName.equals(meta.getJdbcPasswordField())) {
              logDebug("Found the jdbcPasswordField field at index: " + i);
              data.jdbcPasswordField = i;
            }
          }
          //store indices for argument fields
          if (argumentSourceFields) {
            logDebug("Trying to match argument fields against: " + fieldName);
            for (int j = 0; j < argc; j++){
              stringArgument = arguments[j];
              logDebug("Found argument " + j + ": " + stringArgument);
              if (fieldName.equals(stringArgument)) {
                logDebug("Match, storing index " + i);
                data.argumentFieldIndices[j] = i;
              }
            }
          }
        } // end fields loop
        
        //ensure that we have all required fields.
        if (
          (
            JdbcMetaDataMeta.connectionSourceOptionJDBCFields.equals(connectionSource) && (
            data.jdbcDriverField == -1 || 
            data.jdbcUrlField == -1 || 
            data.jdbcUserField == -1 || 
            data.jdbcPasswordField == -1
          )) || (
            JdbcMetaDataMeta.connectionSourceOptionConnectionField.equals(connectionSource) && 
            data.connectionField == -1
          )
        ) {
            throw new KettleException("Not all fields for the connection source were found.");
        }

        if (argumentSourceFields) {
          //keep track of how many fields we used as args.
          //We need this in case remove argument fields is enabled
          //as this is the number of fields we need to discard from the input row. 
          int fieldsUsedAsArgs = 0;
          //ensure all argument fields are bound to a valid field
          argumentFields: for (int j = 0; j < argc; j++) {
            logDebug("Argument indices at " + j + ": " + data.argumentFieldIndices[j]);
            if (data.argumentFieldIndices[j] == -1) {
              //this argument does not point to any existing field.
              if (arguments[j] == null || arguments[j].length() == 0) {
                //the argument is blank, this is ok: we will pass null instead
                data.argumentFieldIndices[j] = -2;
              }
              else {
                //this argument is not blank - this means it points to a non-existing field.
                //this is an error.
                Object[] descriptor = meta.getMethodDescriptor();
                Object[] args = (Object [])descriptor[1];
                Object[] arg = (Object[])args[j];
                throw new KettleException("No field \"" + arguments[j] + "\" found for argument " + j + ": " + ((String)arg[0]));
              }
            }
            else {
              //this argument points to a valid field.
              //let's check if this same field was already used as arg: 
              for (int i = 0; i < j; i++){
                if (data.argumentFieldIndices[i] == data.argumentFieldIndices[j]) {
                  //yes, it was used already. Let's check the next argument.
                  continue argumentFields;
                }
              }
              //this field was not used already, so mark it as used.
              fieldsUsedAsArgs++;
            }
          }
          
          if (meta.getRemoveArgumentFields()) {
            int n = data.outputRowOffset;
            data.outputRowOffset -= fieldsUsedAsArgs;
            data.inputFieldsToCopy = new int[data.outputRowOffset];
            
            inputFieldsToCopy: for (int i = 0, j = 0; i < n; i++) { //for each field in the input row
              for (int k = 0; k < argc; k++) {  //for each method argument
                if (data.argumentFieldIndices[k] == i) {
                  //this input field is used as argument. Continue to the next field. 
                  continue inputFieldsToCopy;
                }
              }
              //this field was not used as argument. make sure we copy it to the output.
              data.inputFieldsToCopy[j++] = i;
            }
          }
        }
        logDebug("Done looking up indices of input fields.");
      }
      
      // clone the input row structure and place it in our data object
      data.outputRowMeta = (RowMetaInterface) inputRowMeta.clone();
      // use meta.getFields() to change it, so it reflects the output row structure 
      meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
    } // end of first
    
    try {
      logRowlevel("Processing 1 input row");
      Connection connection = getConnection(meta, data, r);
      prepareMethodArguments(meta, data, r);
      if (getLogLevel() == LogLevel.ROWLEVEL){
        logRowlevel("About to invoke method");
        for (int i = 0; i < data.arguments.length; i++){
          logRowlevel("Argument " + i +"; " + (data.arguments[i] == null ? "null" : data.arguments[i].toString() + "; " + data.arguments[i].getClass().getName()));
        }
      }
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      ResultSet resultSet = (ResultSet)data.method.invoke(databaseMetaData, data.arguments);
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int columnCount = resultSetMetaData.getColumnCount();
      ValueMetaInterface valueMetaInterface;
      Object value;
      boolean outputRows = false;
      int k;
      while (resultSet.next()) {
        logRowlevel("Processing 1 output row.");
        Object[] outputRow = createOutputRow(meta, data, r);
        for (int i = data.outputRowOffset, j = 0; i < data.outputRowMeta.size(); i++, j++) {
          k = data.resultSetIndices[j];
          if (k > columnCount) continue;
          valueMetaInterface = data.outputRowMeta.getValueMeta(i);
          switch (valueMetaInterface.getType()) {
            case ValueMetaInterface.TYPE_BOOLEAN: //while the JDBC spec prescribes boolean, not all drivers actually can deliver.
              boolean v;
              switch (resultSetMetaData.getColumnType(k)) {
                case Types.INTEGER:
                case Types.SMALLINT:
                case Types.TINYINT:
                  v = resultSet.getInt(k) == 1 ? true : false;
                  break;
                default:
                  v = resultSet.getBoolean(k);
              }
              value = new Boolean(v);
              break;
            case ValueMetaInterface.TYPE_INTEGER:
              value = new Long(resultSet.getInt(k));
              break;
            default:
              value = resultSet.getObject(k);
          }
          outputRow[i] = value;
          outputRows = true;
        }
        // put the row to the output row stream
        putRow(data.outputRowMeta, outputRow); 
        logRowlevel("Done processing 1 output row.");
      }
      resultSet.close();
      if (!outputRows && meta.getAlwaysPassInputRow()) {
        Object[] outputRow = createOutputRow(meta, data, r);
        putRow(data.outputRowMeta, outputRow); 
      }
      logRowlevel("Done processing 1 input row.");
    } 
    catch (Exception exception) {
      exception.printStackTrace();
      if (exception instanceof KettleException) {
        throw (KettleException)exception;
      }
      else {
        throw new KettleException(exception);
      }
    }

    // log progress if it is time to to so
    if (checkFeedback(getLinesRead())) {
      logBasic("Linenr " + getLinesRead()); // Some basic logging
    }

    // indicate that processRow() should be called again
    return true;
  }

  /**
   * This method is called by PDI once the step is done processing. 
   * 
   * The dispose() method is the counterpart to init() and should release any resources
   * acquired for step execution like file handles or database connections.
   * 
   * The meta and data implementations passed in can safely be cast
   * to the step's respective implementations. 
   * 
   * It is mandatory that super.dispose() is called to ensure correct behavior.
   * 
   * @param smi 	step meta interface implementation, containing the step settings
   * @param sdi	step data interface implementation, used to store runtime information
   */
  public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
    // Casting to step-specific implementation classes is safe
    JdbcMetaDataMeta meta = (JdbcMetaDataMeta) smi;
    JdbcMetaDataData data = (JdbcMetaDataData) sdi;
    
    //clean up the database
    try {
      if (data.database != null) {
        data.database.disconnect();
        data.connection = null;
        data.database = null;
      }
    }
    catch (Exception ex){
      logError("Error cleaning up database: " + ex.getMessage());
    }
    
    //clean up the connection
    try {
      if (data.connection != null && !data.connection.isClosed()) {
        data.connection.close();
      }
    }
    catch (Exception ex) {
      logError("Error cleaning up connection: " + ex.getMessage());
    }
    data.connection = null;

    //clean up the database map
    Iterator<Entry<String, Database>> dbIterator = data.databases.entrySet().iterator();
    while (dbIterator.hasNext()) {
      Entry<String, Database> current = dbIterator.next();
      Database database = current.getValue();
      try {
        database.disconnect();
      }
      catch (Exception ex) {
        logError("Error cleaning up database " + current.getKey() + ": " + ex.getMessage());
      }
    }
    data.databases.clear();
    data.databases = null;
      
    //clean up the connection map
    Iterator<Entry<String[], Connection>> connectionIterator = data.connections.entrySet().iterator();
    while (connectionIterator.hasNext()){
      Connection connection = connectionIterator.next().getValue();
      try {
        if (!connection.isClosed()) {
          connection.close();
        }
      }
      catch (Exception ex){
        logError("Error cleaning up connection: " + ex.getMessage());
      }
    }
    data.connections.clear();
    data.connections = null;
    
    data.arguments = null;
    data.method = null;
    data.argumentFieldIndices = null;
    data.inputFieldsToCopy = null;
    data.resultSetIndices = null;
    data.connectionKey = null;

    data.jdbcDriverField = -1;
    data.jdbcUrlField = -1;
    data.jdbcUserField = -1;
    data.jdbcPasswordField = -1;

    super.dispose(meta, data);
  }

}
