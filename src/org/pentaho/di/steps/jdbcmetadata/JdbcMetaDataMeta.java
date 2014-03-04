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

import java.lang.reflect.Method;
import java.sql.DatabaseMetaData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

/**
 * This class is part of the demo step plug-in implementation.
 * It demonstrates the basics of developing a plug-in step for PDI. 
 * 
 * The demo step adds a new string field to the row stream and sets its
 * value to "Hello World!". The user may select the name of the new field.
 *   
 * This class is the implementation of StepMetaInterface.
 * Classes implementing this interface need to:
 * 
 * - keep track of the step settings
 * - serialize step settings both to xml and a repository
 * - provide new instances of objects implementing StepDialogInterface, StepInterface and StepDataInterface
 * - report on how the step modifies the meta-data of the row-stream (row structure and field types)
 * - perform a sanity-check on the settings provided by the user 
 * 
 */
public class JdbcMetaDataMeta extends BaseStepMeta implements StepMetaInterface {
  
  public static Class<DatabaseMetaData> DatabaseMetaDataClass;
  
  private final static Map<Integer, String> OPTIONS_SCOPE = new HashMap<Integer, String>();
  static {
    OPTIONS_SCOPE.put(DatabaseMetaData.bestRowSession, "bestRowSession");
    OPTIONS_SCOPE.put(DatabaseMetaData.bestRowTemporary, "bestRowTemporary");
    OPTIONS_SCOPE.put(DatabaseMetaData.bestRowTransaction, "bestRowTransaction");
  }
  
  //following list of COL_ static members represent columns of metadata result sets
  private final static ValueMeta COL_TABLE_CAT = new ValueMeta("TABLE_CAT", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_PKTABLE_CAT = new ValueMeta("PKTABLE_CAT", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_FKTABLE_CAT = new ValueMeta("FKTABLE_CAT", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_TABLE_CATALOG = new ValueMeta("TABLE_CATALOG", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_TABLE_SCHEM = new ValueMeta("TABLE_SCHEM", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_PKTABLE_SCHEM = new ValueMeta("PKTABLE_SCHEM", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_FKTABLE_SCHEM = new ValueMeta("FKTABLE_SCHEM", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_TABLE_NAME = new ValueMeta("TABLE_NAME", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_PKTABLE_NAME = new ValueMeta("PKTABLE_NAME", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_FKTABLE_NAME = new ValueMeta("FKTABLE_NAME", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_TABLE_TYPE = new ValueMeta("TABLE_TYPE", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_COLUMN_NAME = new ValueMeta("COLUMN_NAME", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_PKCOLUMN_NAME = new ValueMeta("PKCOLUMN_NAME", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_FKCOLUMN_NAME = new ValueMeta("FKCOLUMN_NAME", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_PK_NAME = new ValueMeta("PK_NAME", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_FK_NAME = new ValueMeta("FK_NAME", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_KEY_SEQ = new ValueMeta("KEY_SEQ", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_UPDATE_RULE = new ValueMeta("UPDATE_RULE", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_DELETE_RULE = new ValueMeta("DELETE_RULE", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_DEFERRABILITY = new ValueMeta("DEFERRABILITY", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_TYPE_NAME = new ValueMeta("TYPE_NAME", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_DATA_TYPE = new ValueMeta("DATA_TYPE", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_PRECISION = new ValueMeta("PRECISION", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_COLUMN_SIZE = new ValueMeta("COLUMN_SIZE", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_BUFFER_LENGTH = new ValueMeta("BUFFER_LENGTH", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_LITERAL_PREFIX = new ValueMeta("LITERAL_PREFIX", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_LITERAL_SUFFIX = new ValueMeta("LITERAL_SUFFIX", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_CREATE_PARAMS = new ValueMeta("CREATE_PARAMS", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_NULLABLE = new ValueMeta("NULLABLE", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_CASE_SENSITIVE = new ValueMeta("CASE_SENSITIVE", ValueMetaInterface.TYPE_BOOLEAN);
  private final static ValueMeta COL_SEARCHABLE = new ValueMeta("SEARCHABLE", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_UNSIGNED_ATTRIBUTE = new ValueMeta("UNSIGNED_ATTRIBUTE", ValueMetaInterface.TYPE_BOOLEAN);
  private final static ValueMeta COL_FIXED_PREC_SCALE = new ValueMeta("FIXED_PREC_SCALE", ValueMetaInterface.TYPE_BOOLEAN);
  private final static ValueMeta COL_AUTO_INCREMENT = new ValueMeta("AUTO_INCREMENT", ValueMetaInterface.TYPE_BOOLEAN);
  private final static ValueMeta COL_LOCAL_TYPE_NAME = new ValueMeta("LOCAL_TYPE_NAME", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_MINIMUM_SCALE = new ValueMeta("MINIMUM_SCALE", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_MAXIMUM_SCALE = new ValueMeta("MAXIMUM_SCALE", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_DECIMAL_DIGITS = new ValueMeta("DECIMAL_DIGITS", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_SQL_DATA_TYPE = new ValueMeta("SQL_DATA_TYPE", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_SQL_DATETIME_SUB = new ValueMeta("SQL_DATETIME_SUB", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_SOURCE_DATA_TYPE = new ValueMeta("SOURCE_DATA_TYPE", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_NUM_PREC_RADIX = new ValueMeta("NUM_PREC_RADIX", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_REMARKS = new ValueMeta("REMARKS", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_TYPE_CAT = new ValueMeta("TYPE_CAT", ValueMetaInterface.TYPE_STRING); 
  private final static ValueMeta COL_TYPE_SCHEM = new ValueMeta("TYPE_SCHEM", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_SELF_REFERENCING_COL_NAME = new ValueMeta("SELF_REFERENCING_COL_NAME", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_REF_GENERATION = new ValueMeta("REF_GENERATION", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_SCOPE = new ValueMeta("SCOPE", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_PSEUDO_COLUMN = new ValueMeta("COL_PSEUDO_COLUMN", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_GRANTOR = new ValueMeta("GRANTOR", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_GRANTEE = new ValueMeta("GRANTEE", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_PRIVILEGE = new ValueMeta("PRIVILEGE", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_IS_GRANTABLE = new ValueMeta("IS_GRANTABLE", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_COLUMN_DEF = new ValueMeta("COLUMN_DEF", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_CHAR_OCTET_LENGTH = new ValueMeta("CHAR_OCTET_LENGTH", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_ORDINAL_POSITION = new ValueMeta("ORDINAL_POSITION", ValueMetaInterface.TYPE_INTEGER);
  private final static ValueMeta COL_IS_NULLABLE = new ValueMeta("IS_NULLABLE", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_SCOPE_CATALOG = new ValueMeta("SCOPE_CATALOG", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_SCOPE_SCHEMA = new ValueMeta("SCOPE_SCHEMA", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_SCOPE_TABLE = new ValueMeta("SCOPE_TABLE", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_IS_AUTOINCREMENT = new ValueMeta("IS_AUTOINCREMENT", ValueMetaInterface.TYPE_STRING);
  private final static ValueMeta COL_IS_GENERATEDCOLUMN = new ValueMeta("IS_GENERATEDCOLUMN", ValueMetaInterface.TYPE_STRING);

  //following of argument descriptors describe arguments to metdata methods
  //1) name of the argument
  //2) java type of the argument.
  private final static Object[] ARG_CATALOG = new Object[]{"catalog", String.class};
  private final static Object[] ARG_SCHEMA = new Object[]{"schema", String.class};
  private final static Object[] ARG_TABLE = new Object[]{"table", String.class};
  private final static Object[] ARG_COLUMN_NAME_PATTERN = new Object[]{"columnNamePattern", String.class};
  private final static Object[] ARG_NULLABLE = new Object[]{"nullable", Boolean.class, new Object[]{}};
  private final static Object[] ARG_SCHEMA_PATTERN = new Object[]{"schemaPattern", String.class};
  private final static Object[] ARG_SCOPE = new Object[]{"scope", Integer.class, OPTIONS_SCOPE};
  private final static Object[] ARG_TABLE_TYPES = new Object[]{"tableTypes", String[].class};
  private final static Object[] ARG_TABLE_NAME_PATTERN = new Object[]{"tableNamePattern", String.class};
  private final static Object[] ARG_PARENT_CATALOG = new Object[]{"parentCatalog", String.class};
  private final static Object[] ARG_PARENT_SCHEMA = new Object[]{"parentSchema", String.class};
  private final static Object[] ARG_PARENT_TABLE = new Object[]{"parentTable", String.class};
  private final static Object[] ARG_FOREIGN_CATALOG = new Object[]{"foreignCatalog", String.class};
  private final static Object[] ARG_FOREIGN_SCHEMA = new Object[]{"foreignSchema", String.class};
  private final static Object[] ARG_FOREIGN_TABLE = new Object[]{"foreignTable", String.class};
  
  //this is a map of the methods we can get metadata from.
  //1) name of the java.sql.DatabaseMetaData method
  //2) array of argument descriptors
  //3) array of return fields
  //4) initially empty slot where the actual Method object is lazily stored.
  public final static Object[] methodDescriptors = new Object[]{
    new Object[]{
      "getCatalogs", 
      new Object[]{}, 
      new ValueMetaInterface[]{COL_TABLE_CAT},
      null
    },
    new Object[]{
      "getBestRowIdentifier", 
      new Object[]{ARG_CATALOG, ARG_SCHEMA, ARG_TABLE, ARG_SCOPE, ARG_NULLABLE},
      new ValueMetaInterface[]{
        COL_SCOPE, COL_COLUMN_NAME, COL_DATA_TYPE, COL_TYPE_NAME, 
        COL_COLUMN_SIZE, COL_BUFFER_LENGTH, COL_DECIMAL_DIGITS, COL_PSEUDO_COLUMN
      },
      null
    },
    new Object[]{
      "getColumnPrivileges", 
      new Object[]{ARG_CATALOG, ARG_SCHEMA, ARG_TABLE, ARG_COLUMN_NAME_PATTERN},
      new ValueMetaInterface[]{
        COL_TABLE_CAT, COL_TABLE_SCHEM, COL_TABLE_NAME, COL_COLUMN_NAME, 
        COL_GRANTOR, COL_GRANTEE, COL_PRIVILEGE, COL_IS_GRANTABLE
      },
      null
    },
    new Object[]{
      "getColumns", 
      new Object[]{ARG_CATALOG, ARG_SCHEMA_PATTERN, ARG_TABLE_NAME_PATTERN, ARG_COLUMN_NAME_PATTERN},
      new ValueMetaInterface[]{
        COL_TABLE_CAT, COL_TABLE_SCHEM, COL_TABLE_NAME, COL_COLUMN_NAME, 
        COL_DATA_TYPE, COL_TYPE_NAME, COL_COLUMN_SIZE, COL_BUFFER_LENGTH, COL_DECIMAL_DIGITS, COL_NUM_PREC_RADIX,
        COL_NULLABLE, COL_REMARKS, COL_COLUMN_DEF, COL_SQL_DATA_TYPE, COL_SQL_DATETIME_SUB, COL_CHAR_OCTET_LENGTH,
        COL_ORDINAL_POSITION, COL_IS_NULLABLE, 
        COL_SCOPE_CATALOG, COL_SCOPE_SCHEMA, COL_SCOPE_TABLE, 
        COL_SOURCE_DATA_TYPE, COL_IS_AUTOINCREMENT, COL_IS_GENERATEDCOLUMN
      },
      null
    },
    new Object[]{
      "getCrossReference", 
      new Object[]{
        ARG_PARENT_CATALOG, ARG_PARENT_SCHEMA, ARG_PARENT_TABLE,
        ARG_FOREIGN_CATALOG, ARG_FOREIGN_SCHEMA, ARG_FOREIGN_TABLE,
      },
      new ValueMetaInterface[]{
        COL_PKTABLE_CAT, COL_PKTABLE_SCHEM, COL_PKTABLE_NAME, COL_PKCOLUMN_NAME, 
        COL_FKTABLE_CAT, COL_FKTABLE_SCHEM, COL_FKTABLE_NAME, COL_FKCOLUMN_NAME, 
        COL_KEY_SEQ, COL_UPDATE_RULE, COL_DELETE_RULE, COL_FK_NAME, COL_PK_NAME, COL_DEFERRABILITY
      },
      null
    },
    new Object[]{
      "getExportedKeys", 
      new Object[]{ARG_CATALOG, ARG_SCHEMA, ARG_TABLE},
      new ValueMetaInterface[]{
        COL_PKTABLE_CAT, COL_PKTABLE_SCHEM, COL_PKTABLE_NAME, COL_PKCOLUMN_NAME, 
        COL_FKTABLE_CAT, COL_FKTABLE_SCHEM, COL_FKTABLE_NAME, COL_FKCOLUMN_NAME, 
        COL_KEY_SEQ, COL_UPDATE_RULE, COL_DELETE_RULE, COL_FK_NAME, COL_PK_NAME, COL_DEFERRABILITY
      },
      null
    },
    new Object[]{
      "getImportedKeys", 
      new Object[]{ARG_CATALOG, ARG_SCHEMA, ARG_TABLE},
      new ValueMetaInterface[]{
        COL_PKTABLE_CAT, COL_PKTABLE_SCHEM, COL_PKTABLE_NAME, COL_PKCOLUMN_NAME, 
        COL_FKTABLE_CAT, COL_FKTABLE_SCHEM, COL_FKTABLE_NAME, COL_FKCOLUMN_NAME, 
        COL_KEY_SEQ, COL_UPDATE_RULE, COL_DELETE_RULE, COL_FK_NAME, COL_PK_NAME, COL_DEFERRABILITY
      },
      null
    },
    new Object[]{
      "getPrimaryKeys", 
      new Object[]{ARG_CATALOG, ARG_SCHEMA, ARG_TABLE},
      new ValueMetaInterface[]{COL_TABLE_CAT, COL_TABLE_SCHEM, COL_TABLE_NAME, COL_COLUMN_NAME,COL_KEY_SEQ, COL_PK_NAME},
      null
    },
    new Object[]{
      "getSchemas", 
      new Object[]{}, 
      new ValueMetaInterface[]{COL_TABLE_SCHEM, COL_TABLE_CATALOG},
      null
    },
    /*  We'd love to use this version of getSchemas, but we found that calling it throws AbstractMethodError in h2 and sqlite (possibly others)
    new Object[]{
      "getSchemas", 
      new Object[]{ARG_CATALOG, ARG_SCHEMA_PATTERN}, 
      new ValueMetaInterface[]{COL_TABLE_SCHEM, COL_TABLE_CATALOG},
      null
    },
    */
    new Object[]{
      "getTablePrivileges", 
      new Object[]{ARG_CATALOG, ARG_SCHEMA_PATTERN, ARG_TABLE_NAME_PATTERN},
      new ValueMetaInterface[]{
        COL_TABLE_CAT, COL_TABLE_SCHEM, COL_TABLE_NAME, 
        COL_GRANTOR, COL_GRANTEE, COL_PRIVILEGE, COL_IS_GRANTABLE
      },
      null
    },
    new Object[]{
      "getTableTypes", 
      new Object[]{}, 
      new ValueMetaInterface[]{COL_TABLE_TYPE},
      null
    },
    new Object[]{
      "getTables", 
      new Object[]{ARG_CATALOG, ARG_SCHEMA_PATTERN, ARG_TABLE_NAME_PATTERN, ARG_TABLE_TYPES}, 
      new ValueMetaInterface[]{
        COL_TABLE_CAT, COL_TABLE_SCHEM, COL_TABLE_NAME, COL_TABLE_TYPE, COL_REMARKS, 
        COL_TYPE_CAT, COL_TYPE_SCHEM, COL_TYPE_NAME, COL_SELF_REFERENCING_COL_NAME,
        COL_REF_GENERATION
      },
      null
    },
    new Object[]{
      "getTypeInfo", 
      new Object[]{}, 
      new ValueMetaInterface[]{
        COL_TYPE_NAME, COL_DATA_TYPE, COL_PRECISION, COL_LITERAL_PREFIX, COL_LITERAL_SUFFIX, 
        COL_CREATE_PARAMS, COL_NULLABLE, COL_CASE_SENSITIVE, COL_SEARCHABLE, COL_UNSIGNED_ATTRIBUTE, 
        COL_FIXED_PREC_SCALE, COL_AUTO_INCREMENT, COL_LOCAL_TYPE_NAME, 
        COL_MINIMUM_SCALE, COL_MAXIMUM_SCALE, COL_SQL_DATA_TYPE, COL_SQL_DATETIME_SUB, COL_NUM_PREC_RADIX
      },
      null
    },
    new Object[]{
      "getVersionColumns", 
      new Object[]{ARG_CATALOG, ARG_SCHEMA, ARG_TABLE},
      new ValueMetaInterface[]{
        COL_SCOPE, COL_COLUMN_NAME, COL_DATA_TYPE, COL_TYPE_NAME, 
        COL_COLUMN_SIZE, COL_BUFFER_LENGTH, COL_DECIMAL_DIGITS, COL_PSEUDO_COLUMN
      },
      null
    }
  };
  
  private final static String CONNECTION_SOURCE = "connectionSource";
  private final static String CONNECTION_NAME = "connectionName";
  private final static String CONNECTION_FIELD = "connectionField";
  private final static String JDBC_DRIVER_FIELD = "jdbcDriverField";
  private final static String JDBC_URL_FIELD = "jdbcUrlField";
  private final static String JDBC_USER_FIELD = "jdbcUserField";
  private final static String JDBC_PASSWORD_FIELD = "jdbcPasswordField";
  private final static String ALWAYS_PASS_INPUT_ROW = "alwaysPassInputRow";
  private final static String METHOD_NAME = "methodName";
  private final static String REMOVE_ARGUMENT_FIELDS = "removeArgumentFields";
  private final static String ARGUMENT_SOURCE_FIELDS = "argumentSourceFields";
  private final static String ARGUMENT = "argument";
  private final static String ARGUMENTS = ARGUMENT + "s";
  private final static String OUTPUT_FIELD = "outputField";
  private final static String OUTPUT_FIELDS = OUTPUT_FIELD + "s";
  private final static String FIELD_NAME = "name";
  private final static String FIELD_RENAME = "rename";

  /**
   *  The PKG member is used when looking up internationalized strings.
   *  The properties file with localized keys is expected to reside in 
   *  {the package of the class specified}/messages/messages_{locale}.properties   
   */
  private static Class<?> PKG = JdbcMetaDataMeta.class; // for i18n purposes

  public static final String connectionSourceOptionConnection = "Connection";
  public static final String connectionSourceOptionConnectionField = "ConnectionField";
  public static final String connectionSourceOptionJDBC = "JDBC";
  public static final String connectionSourceOptionJDBCFields= "JDBCFields";

  public static final String[] connectionSourceOptions = new String[]{
    connectionSourceOptionConnection, 
    connectionSourceOptionConnectionField, 
    connectionSourceOptionJDBC, 
    connectionSourceOptionJDBCFields
  };
  
  /**
   * Constructor should call super() to make sure the base class has a chance to initialize properly.
   */
  public JdbcMetaDataMeta() {
    super();
    if (JdbcMetaDataMeta.DatabaseMetaDataClass == null) {
      try {
        JdbcMetaDataMeta.DatabaseMetaDataClass = (Class<DatabaseMetaData>)Class.forName("java.sql.DatabaseMetaData");
      }
      catch (Exception exception){
        throw new IllegalArgumentException(exception);
      }
    }
  }

  /**
   * Called by Spoon to get a new instance of the SWT dialog for the step.
   * A standard implementation passing the arguments to the constructor of the step dialog is recommended.
   * 
   * @param shell an SWT Shell
   * @param meta  description of the step 
   * @param transMeta	description of the the transformation 
   * @param name the name of the step
   * @return  new instance of a dialog for this step 
   */
  public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
    return new JdbcMetaDataDialog(shell, meta, transMeta, name);
  }

  /**
   * Called by PDI to get a new instance of the step implementation. 
   * A standard implementation passing the arguments to the constructor of the step class is recommended.
   * 
   * @param stepMeta description of the step
   * @param stepDataInterface instance of a step data class
   * @param cnr copy number
   * @param transMeta description of the transformation
   * @param disp runtime implementation of the transformation
   * @return the new instance of a step implementation 
   */
  public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp) {
    return new JdbcMetaData(stepMeta, stepDataInterface, cnr, transMeta, disp);
  }

  /**
   * Called by PDI to get a new instance of the step data class.
   */
  public StepDataInterface getStepData() {
    return new JdbcMetaDataData();
  }

  /**
   * This method is called every time a new step is created and should allocate/set the step configuration
   * to sensible defaults. The values set here will be used by Spoon when a new step is created.    
   */
  public void setDefault() {
    connectionSource = "Connection";
    connectionName = "";
    jdbcDriverField = "";
    jdbcUrlField = "";
    jdbcUserField = "";
    jdbcPasswordField = "";
    methodName = "getCatalogs";
    argumentSourceFields = false;
  }

  /**
   * Stores the connectionsource. 
   */
  private String connectionSource;
  /**
   * Get the index of the connection source option
   * @param connectionSourceOption
   * @return
   */
  public static int getConnectionSourceOptionIndex(String connectionSourceOption){
    for (int i = 0; i < connectionSourceOptions.length; i++) {
      if (connectionSourceOptions[i].equals(connectionSourceOption)) return i;
    }
    return -1;
  }
  /**
   * Getter for the name of the field containing the connection source
   * @return the source of the connection data
   */
  public String getConnectionSource() {
    return connectionSource;
  }
  /**
   * Setter for the name of the field containing the connection source
   * @param connectionSource the source for  the connection data
   */
  public void setConnectionSource(String connectionSource) {
    if (connectionSource == null) connectionSource = connectionSourceOptions[0];
    else
    if (getConnectionSourceOptionIndex(connectionSource) == -1) throw new IllegalArgumentException(
      connectionSource + " is not a valid value for connectionSource."
    );
    this.connectionSource = connectionSource;
  }

  /**
   * Stores the name of connection. 
   */
  private String connectionName;
  /**
   * Getter for the name of the connection
   * @return the name of the connection
   */
  public String getConnectionName() {
    return connectionName;
  }
  /**
   * Setter for the name of the connection
   * @param connectionName the name of the connection
   */
  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }

  /**
   * Stores the name of field holding the connection name. 
   */
  private String connectionField;
  /**
   * Getter for the name of the field holding the connection name
   * @return the name of the field holding the connection name
   */
  public String getConnectionField() {
    return connectionField;
  }
  /**
   * Setter for the name of the field holding the name of the connection
   * @param connectionField the name of the field holding the connection name
   */
  public void setConnectionField(String connectionField) {
    this.connectionField = connectionField;
  }
  /**
   * Stores the name of the field containing the name of the jdbc driver. 
   */
  private String jdbcDriverField;
  /**
   * Getter for the name of the field containing the jdbc driver
   * @return the name of the field containing the jdbc driver
   */
  public String getJdbcDriverField() {
    return jdbcDriverField;
  }
  /**
   * Setter for the name of the field containing the jdbc driver
   * @param jdbcDriverField the name of the field containing the jdbc driver
   */
  public void setJdbcDriverField(String jdbcDriverField) {
    this.jdbcDriverField = jdbcDriverField;
  }

  /**
   * Stores the name of the field containing the url for the jdbc connection. 
   */
  private String jdbcUrlField;
  /**
   * Getter for the name of the field containing the jdbc url
   * @return the name of the field containing the jdbc url
   */
  public String getJdbcUrlField() {
    return jdbcUrlField;
  }
  /**
   * Setter for the name of the field containing the jdbc url
   * @param jdbcUrlField the name of the field containing the jdbc url
   */
  public void setJdbcUrlField(String jdbcUrlField) {
    this.jdbcUrlField = jdbcUrlField;
  }

  /**
   * Stores the name of the field containing the username for the jdbc connection. 
   */
  private String jdbcUserField;
  /**
   * Getter for the name of the field containing the jdbc user
   * @return the name of the field containing the jdbc user
   */
  public String getJdbcUserField() {
    return jdbcUserField;
  }
  /**
   * Setter for the name of the field containing the jdbc user
   * @param jdbcDriverField the name of the field containing the jdbc user
   */
  public void setJdbcUserField(String jdbcUserField) {
    this.jdbcUserField = jdbcUserField;
  }

  /**
   * Stores the name of the field containing the password for the jdbc connection. 
   */
  private String jdbcPasswordField;
  /**
   * Getter for the name of the field containing the jdbc password
   * @return the name of the field containing the jdbc password
   */
  public String getJdbcPasswordField() {
    return jdbcPasswordField;
  }
  /**
   * Setter for the name of the field containing the jdbc password
   * @param jdbcDriverField the name of the field containing the jdbc password
   */
  public void setJdbcPasswordField(String jdbcPasswordField) {
    this.jdbcPasswordField = jdbcPasswordField;
  }

  /**
   * Stores the whether the input row should be returned even if no metadata was found
   */
  private boolean alwaysPassInputRow;
  /**
   * @return whether fields are to be used as arguments
   */
  public boolean getAlwaysPassInputRow() {
    return alwaysPassInputRow;
  }
  /**
   * @param argumentSourceFields whether fields should be used as arguments
   */
  public void setAlwaysPassInputRow(boolean alwaysPassInputRow) {
    this.alwaysPassInputRow = alwaysPassInputRow;
  }

  /**
   * Stores the name of the method used to get the metadata
   */
  private String methodName;
  /**
   * Getter for the name method used to get metadata
   * @return the name of the method used to get metadata
   */
  public String getMethodName() {
    return methodName;
  }
  /**
   * Setter for the name of the method used to get metadata
   * @param methodName the name of the method used to get metadata
   */
  public void setMethodName(String methodName) {
    this.methodName = methodName;
  }

  private static Object[] getMethodDescriptor(String methodName) {
    for (Object o : JdbcMetaDataMeta.methodDescriptors) {
      Object[] oo = (Object[])o;
      if (!oo[0].toString().equals(methodName)) continue;
      return oo;
    }
    return null;
  }

  public static Object[] getMethodDescriptor(int index) {
    return (Object[])JdbcMetaDataMeta.methodDescriptors[index];
  }

  public Object[] getMethodDescriptor() {
    return JdbcMetaDataMeta.getMethodDescriptor(getMethodName());
  }

  public ValueMetaInterface[] getMethodResultSetDescriptor(){
    Object[] methodDescriptor = getMethodDescriptor();
    return (ValueMetaInterface[]) methodDescriptor[2];
  }

  public static int getMethodDescriptorIndex(String methodName){
    Object[] methods = JdbcMetaDataMeta.methodDescriptors;
    int n = methods.length;
    Object[] methodDescriptor;
    String name;
    for (int i = 0; i < n; i++) {
      methodDescriptor = (Object[])methods[i];
      name = (String)methodDescriptor[0];
      if (name.equals(methodName)) return i;
    }
    return -1;
  }
  
  public static String getMethodName(int methodDescriptorIndex) {
    Object[] methodDescriptor = (Object[])JdbcMetaDataMeta.methodDescriptors[methodDescriptorIndex];
    String methodName = (String)methodDescriptor[0];
    return methodName;
  }

  public static Class<?>[] getMethodParameterTypes(Object[] methodDescriptor) {
    Object[] parameters = (Object[])methodDescriptor[1];
    Object[] parameter;
    int n = parameters.length;
    Class<?>[] parameterTypes = new Class<?>[n];
    for (int i = 0; i < n; i++) {
      parameter = (Object[])parameters[i];
      parameterTypes[i] = (Class<?>)parameter[1];
    }
    return parameterTypes;
  }

  public static Method getMethod(String methodName) throws Exception {
    Method method;
    Object[] methodDescriptor = getMethodDescriptor(methodName);
    method = (Method)methodDescriptor[3];
    if (method != null) return method;
    Class<?> dbmd = Class.forName("java.sql.DatabaseMetaData");
    Class<?>[] parameterTypes = getMethodParameterTypes(methodDescriptor);
    method = dbmd.getDeclaredMethod(methodName, parameterTypes);
    methodDescriptor[3] = method;
    return method;
  }

  public Method getMethod() throws Exception {
    return JdbcMetaDataMeta.getMethod(methodName);
  }

  /**
   * Stores the whether fields are to be used for method arguments
   */
  private boolean argumentSourceFields;
  /**
   * @return whether fields are to be used as arguments
   */
  public boolean getArgumentSourceFields() {
    return argumentSourceFields;
  }
  /**
   * @param argumentSourceFields whether fields should be used as arguments
   */
  public void setArgumentSourceFields(boolean argumentSourceFields) {
    this.argumentSourceFields = argumentSourceFields;
  }

  /**
   * Stores the whether to remove the fields used as arguments from the output row
   */
  private boolean removeArgumentFields;
  /**
   * @return whether  to remove the fields used as arguments from the output row
   */
  public boolean getRemoveArgumentFields() {
    return removeArgumentFields;
  }
  /**
   * @param removeArgumentFields whether fields used as arguments should be removed from the output row
   */
  public void setRemoveArgumentFields(boolean removeArgumentFields) {
    this.removeArgumentFields = removeArgumentFields;
  }

  /**
   * Stores method arguments
   */
  private String[] arguments;
  /**
   * @return get method arguments
   */
  public String[] getArguments() {
    return arguments;
  }
  /**
   * @param argumentSourceFields whether fields should be used as arguments
   */
  public void setArguments(String[] arguments) {
    this.arguments = arguments;
  }
  
  /**
   * Stores the selection of fields that are added to the stream
   */
  private Object[] outputFields;
  /**
   * @return the selection of fields added to the stream
   */
  public Object[] getOutputFields() {
    return outputFields;
  }
  /**
   * @param argumentSourceFields whether fields should be used as arguments
   */
  public void setOutputFields(Object[] outputFields) {
    this.outputFields = outputFields;
  }

  /**
   * This method is used when a step is duplicated in Spoon. It needs to return a deep copy of this
   * step meta object. Be sure to create proper deep copies if the step configuration is stored in
   * modifiable objects.
   * 
   * See org.pentaho.di.trans.steps.rowgenerator.RowGeneratorMeta.clone() for an example on creating
   * a deep copy.
   * 
   * @return a deep copy of this
   */
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  /**
   * This method is called by Spoon when a step needs to serialize its configuration to XML. The expected
   * return value is an XML fragment consisting of one or more XML tags.  
   * 
   * Please use org.pentaho.di.core.xml.XMLHandler to conveniently generate the XML.
   * 
   * @return a string containing the XML serialization of this step
   */
  public String getXML() throws KettleValueException {		
    // only one field to serialize
    StringBuffer xml = new StringBuffer();
    String indent = "    ";
    xml.append(indent);
    xml.append(XMLHandler.addTagValue(CONNECTION_SOURCE, connectionSource));
    xml.append(indent);
    xml.append(XMLHandler.addTagValue(CONNECTION_NAME, connectionName));
    xml.append(indent);
    xml.append(XMLHandler.addTagValue(CONNECTION_FIELD, connectionField));
    xml.append(indent);
    xml.append(XMLHandler.addTagValue(JDBC_DRIVER_FIELD, jdbcDriverField));
    xml.append(indent);
    xml.append(XMLHandler.addTagValue(JDBC_URL_FIELD, jdbcUrlField));
    xml.append(indent);
    xml.append(XMLHandler.addTagValue(JDBC_USER_FIELD, jdbcUserField));
    xml.append(indent);
    xml.append(XMLHandler.addTagValue(JDBC_PASSWORD_FIELD, jdbcPasswordField));
    xml.append(indent);
    xml.append(XMLHandler.addTagValue(ALWAYS_PASS_INPUT_ROW, alwaysPassInputRow));
    xml.append(indent);
    xml.append(XMLHandler.addTagValue(METHOD_NAME, methodName));
    xml.append(indent);
    xml.append(XMLHandler.addTagValue(REMOVE_ARGUMENT_FIELDS, removeArgumentFields));
    xml.append(indent);
    xml.append(XMLHandler.addTagValue(ARGUMENT_SOURCE_FIELDS, argumentSourceFields));

    xml.append(indent);
    xml.append("<" + ARGUMENTS + ">\n");
    for (int i = 0; i < arguments.length; i++) {
      xml.append(indent + indent);
      xml.append(XMLHandler.addTagValue(ARGUMENT, arguments[i]));
    }
    xml.append(indent);
    xml.append("</" + ARGUMENTS + ">\n");

    xml.append(indent);
    xml.append("<" + OUTPUT_FIELDS + ">\n");
    if (outputFields != null) {
      Object[] outputField;
      for (int i = 0; i < outputFields.length; i++) {
        outputField = (Object[])outputFields[i];
        xml.append(indent + indent);
        xml.append("<" + OUTPUT_FIELD + ">\n");
        xml.append(indent + indent + indent);
        xml.append(XMLHandler.addTagValue(FIELD_NAME, (String)outputField[0]));
        xml.append(indent + indent + indent);
        xml.append(XMLHandler.addTagValue(FIELD_RENAME, (String)outputField[1]));
        xml.append(indent + indent);
        xml.append("</" + OUTPUT_FIELD + ">\n");
      }
    }
    xml.append(indent);
    xml.append("</" + OUTPUT_FIELDS + ">\n");

    return xml.toString();
  }

  /**
   * This method is called by PDI when a step needs to load its configuration from XML.
   * 
   * Please use org.pentaho.di.core.xml.XMLHandler to conveniently read from the
   * XML node passed in.
   * 
   * @param stepnode	the XML node containing the configuration
   * @param databases	the databases available in the transformation
   * @param counters	the counters available in the transformation
   */
  public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleXMLException {
    try {
      setConnectionSource(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, CONNECTION_SOURCE)));
      setConnectionName(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, CONNECTION_NAME)));
      setConnectionField(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, CONNECTION_FIELD)));
      setJdbcDriverField(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, JDBC_DRIVER_FIELD)));
      setJdbcUrlField(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, JDBC_URL_FIELD)));
      setJdbcUserField(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, JDBC_USER_FIELD)));
      setJdbcPasswordField(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, JDBC_PASSWORD_FIELD)));
      setAlwaysPassInputRow("Y".equals(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, ALWAYS_PASS_INPUT_ROW))));
      setMethodName(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, METHOD_NAME)));
      setArgumentSourceFields("Y".equals(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, ARGUMENT_SOURCE_FIELDS))));
      setRemoveArgumentFields("Y".equals(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, REMOVE_ARGUMENT_FIELDS))));

      Node argumentNodes = XMLHandler.getSubNode( stepnode, ARGUMENTS);
      int n;
      n = XMLHandler.countNodes(argumentNodes, ARGUMENT );
      String[] arguments = new String[n];
      for (int i = 0; i < n; i++) {
        Node argumentNode = XMLHandler.getSubNodeByNr(argumentNodes, ARGUMENT, i);
        arguments[i] = XMLHandler.getNodeValue(argumentNode);
      }
      setArguments(arguments);
      
      Node outputFieldNodes = XMLHandler.getSubNode(stepnode, OUTPUT_FIELDS);
      n = XMLHandler.countNodes(outputFieldNodes, OUTPUT_FIELD);
      outputFields = new Object[n];
      String[] outputField;
      for (int i = 0; i < n; i++) {
        Node outputFieldNode = XMLHandler.getSubNodeByNr(outputFieldNodes, OUTPUT_FIELD, i);
        Node outputFieldName = XMLHandler.getSubNode(outputFieldNode, FIELD_NAME);
        Node outputFieldRename = XMLHandler.getSubNode(outputFieldNode, FIELD_RENAME);
        outputField = new String[]{
          XMLHandler.getNodeValue(outputFieldName),
          XMLHandler.getNodeValue(outputFieldRename)
        };
        outputFields[i] = outputField;
      }
    } catch (Exception e) {
      throw new KettleXMLException("Unable to read step info from XML node", e);
    }
  }

  /**
   * This method is called by Spoon when a step needs to serialize its configuration to a repository.
   * The repository implementation provides the necessary methods to save the step attributes.
   *
   * @param rep the repository to save to
   * @param id_transformation the id to use for the transformation when saving
   * @param id_step the id to use for the step  when saving
   */
  public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step) throws KettleException {
    try{
      rep.saveStepAttribute(id_transformation, id_step, CONNECTION_SOURCE, connectionSource);
      rep.saveStepAttribute(id_transformation, id_step, CONNECTION_NAME, connectionName);
      rep.saveStepAttribute(id_transformation, id_step, CONNECTION_FIELD, connectionField);
      rep.saveStepAttribute(id_transformation, id_step, JDBC_DRIVER_FIELD, jdbcDriverField);
      rep.saveStepAttribute(id_transformation, id_step, JDBC_URL_FIELD, jdbcUrlField);
      rep.saveStepAttribute(id_transformation, id_step, JDBC_USER_FIELD, jdbcUserField);
      rep.saveStepAttribute(id_transformation, id_step, JDBC_PASSWORD_FIELD, jdbcPasswordField);
      rep.saveStepAttribute(id_transformation, id_step, ALWAYS_PASS_INPUT_ROW, alwaysPassInputRow);
      rep.saveStepAttribute(id_transformation, id_step, METHOD_NAME, methodName);
      rep.saveStepAttribute(id_transformation, id_step, ARGUMENT_SOURCE_FIELDS, argumentSourceFields);
      rep.saveStepAttribute(id_transformation, id_step, REMOVE_ARGUMENT_FIELDS, removeArgumentFields);
      for (int i = 0; i < arguments.length; i++) {
        rep.saveStepAttribute(id_transformation, id_step, i, ARGUMENT, arguments[i]);
      }
      String[] outputField;
      for (int i = 0; i < outputFields.length; i++) {
        outputField = (String[])outputFields[i];
        rep.saveStepAttribute(id_transformation, id_step, i, FIELD_NAME, outputField[0]);
        rep.saveStepAttribute(id_transformation, id_step, i, FIELD_RENAME, outputField[1]);
      }
    }
    catch(Exception e){
      throw new KettleException("Unable to save step into repository: "+id_step, e); 
    }
  }

  /**
   * This method is called by PDI when a step needs to read its configuration from a repository.
   * The repository implementation provides the necessary methods to read the step attributes.
   * 
   * @param rep		the repository to read from
   * @param id_step	the id of the step being read
   * @param databases	the databases available in the transformation
   * @param counters	the counters available in the transformation
   */
  public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleException {
    try{
      setConnectionSource(rep.getStepAttributeString(id_step, CONNECTION_SOURCE));
      setConnectionName(rep.getStepAttributeString(id_step, CONNECTION_NAME));
      setConnectionField(rep.getStepAttributeString(id_step, CONNECTION_FIELD));
      setJdbcDriverField(rep.getStepAttributeString(id_step, JDBC_DRIVER_FIELD));
      setJdbcUrlField(rep.getStepAttributeString(id_step, JDBC_URL_FIELD));
      setJdbcUserField(rep.getStepAttributeString(id_step, JDBC_USER_FIELD));
      setJdbcPasswordField(rep.getStepAttributeString(id_step, JDBC_PASSWORD_FIELD));
      setAlwaysPassInputRow("Y".equals(rep.getStepAttributeString(id_step, ALWAYS_PASS_INPUT_ROW)));
      setMethodName(rep.getStepAttributeString(id_step, METHOD_NAME));
      setArgumentSourceFields("Y".equals(rep.getStepAttributeString(id_step, ARGUMENT_SOURCE_FIELDS)));
      setRemoveArgumentFields("Y".equals(rep.getStepAttributeString(id_step, REMOVE_ARGUMENT_FIELDS)));

      int n;
      n = rep.countNrStepAttributes(id_step, ARGUMENT);
      arguments = new String[n];
      for (int i = 0; i < n; i++) {
        arguments[i] = rep.getStepAttributeString(id_step, i, ARGUMENT);
      }
      
      n = rep.countNrStepAttributes(id_step, OUTPUT_FIELD);
      outputFields = new Object[n];
      String[] outputField;
      for (int i = 0; i < n; i++) {
        outputField = new String[]{
          rep.getStepAttributeString(id_step, i, FIELD_NAME),
          rep.getStepAttributeString(id_step, i, FIELD_RENAME)
        };
        outputFields[i] = outputField;
      }
    }
    catch(Exception e){
      throw new KettleException("Unable to load step from repository", e);
    }
  }
  
  /**
   * This method is called to determine the changes the step is making to the row-stream.
   * To that end a RowMetaInterface object is passed in, containing the row-stream structure as it is when entering
   * the step. This method must apply any changes the step makes to the row stream. Usually a step adds fields to the
   * row-stream.
   * 
   * @param r			the row structure coming in to the step
   * @param origin	the name of the step making the changes
   * @param info		row structures of any info steps coming in
   * @param nextStep	the description of a step this step is passing rows to
   * @param space		the variable space for resolving variables
   */
  public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
    //remove argument source fields coming from the input
    if (argumentSourceFields && removeArgumentFields){
      for (int i = 0; i < arguments.length; i++) {
        try {
          r.removeValueMeta(arguments[i]);
        }
        catch (KettleException ex) {
          //this probably means the requested field could not be found.
          //we can't really handle this here; however, it's not a problem
          //because a missing field will be detected before writing rows.
        }
      }
    }
    
    //add the outputfields added by this step.
    Object[] outputFields = getOutputFields();
    String[] outputField;
    int n = outputFields.length;
    
    Object[] methodDescriptor = getMethodDescriptor();
    ValueMetaInterface[] fields = (ValueMetaInterface[])methodDescriptor[2];
    int m = fields.length;
    ValueMetaInterface field;
    
    for (int i = 0; i < n; i++) {
      outputField = (String[])outputFields[i];
      for (int j = 0; j < m; j++) {
        field = fields[j];
        if (!outputField[0].equals(field.getName())) continue;
        field = new ValueMeta(outputField[1], field.getType());
        field.setOrigin(origin);
        r.addValueMeta(field);
        break;
      }
    }
  }

  /**
   * This method is called when the user selects the "Verify Transformation" option in Spoon. 
   * A list of remarks is passed in that this method should add to. Each remark is a comment, warning, error, or ok.
   * The method should perform as many checks as necessary to catch design-time errors.
   * 
   * Typical checks include:
   * - verify that all mandatory configuration is given
   * - verify that the step receives any input, unless it's a row generating step
   * - verify that the step does not receive any input if it does not take them into account
   * - verify that the step finds fields it relies on in the row-stream
   *
   *   @param remarks the list of remarks to append to
   *   @param transmeta the description of the transformation
   *   @param stepMeta the description of the step
   *   @param prev the structure of the incoming row-stream
   *   @param input names of steps sending input to the step
   *   @param output names of steps this step is sending output to
   *   @param info fields coming in from info steps 
   */
  public void check(
    List<CheckResultInterface> remarks, 
    TransMeta transmeta, 
    StepMeta stepMeta, 
    RowMetaInterface prev, 
    String input[], 
    String output[], 
    RowMetaInterface info
  ) {
    CheckResult cr;
    // See if there are input streams leading to this step!
    if (input.length > 0) {
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, BaseMessages.getString(PKG, "Demo.CheckResult.ReceivingRows.OK"), stepMeta);
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, "Demo.CheckResult.ReceivingRows.ERROR"), stepMeta);
      remarks.add(cr);
    }
  }

}
