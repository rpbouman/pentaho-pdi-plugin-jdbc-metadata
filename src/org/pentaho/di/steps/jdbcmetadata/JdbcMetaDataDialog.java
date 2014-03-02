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

import java.sql.DatabaseMetaData;
import java.util.List;
import java.util.ArrayList;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.CCombo;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.ComboVar;

import org.pentaho.di.core.exception.KettleException;

/**
 * This class is part of the demo step plug-in implementation.
 * It demonstrates the basics of developing a plug-in step for PDI. 
 * 
 * The demo step adds a new string field to the row stream and sets its
 * value to "Hello World!". The user may select the name of the new field.
 *   
 * This class is the implementation of StepDialogInterface.
 * Classes implementing this interface need to:
 * 
 * - build and open a SWT dialog displaying the step's settings (stored in the step's meta object)
 * - write back any changes the user makes to the step's meta object
 * - report whether the user changed any settings when confirming the dialog 
 * 
 */
public class JdbcMetaDataDialog extends BaseStepDialog implements StepDialogInterface {

  /**
  * The PKG member is used when looking up internationalized strings.
  * The properties file with localized keys is expected to reside in 
  * {the package of the class specified}/messages/messages_{locale}.properties   
  */
  private static Class<?> PKG = JdbcMetaDataMeta.class; // for i18n purposes
  
  // this is the object the stores the step's settings
  // the dialog reads the settings from it when opening
  // the dialog writes the settings to it when confirmed 
  private JdbcMetaDataMeta meta;

  private int middle = props.getMiddlePct();
  private int margin = Const.MARGIN;

  private boolean dialogChanged;
  private ModifyListener lsMod;
  
  private Composite metadataComposite;
  //
  private CCombo connectionSourceCombo;
  //
  private CCombo connectionCombo;
  // text field holding the name of the field containing the connection name
  private CCombo connectionField;
  // text field holding the name of the field containing the driver name
  private ComboVar jdbcDriverField;
  // text field holding the name of the field containing the url
  private ComboVar jdbcUrlField;
  // text field holding the name of the field containing the user
  private ComboVar jdbcUserField;
  // text field holding the name of the field containing the password
  private ComboVar jdbcPasswordField;

  //
  private Label alwaysPassInputRowLabel;
  //
  private Button alwaysPassInputRowButton;
  //
  private Label methodLabel;
  //
  private CCombo methodCombo;
  
  private Label argumentSourceLabel;
  //
  private Button argumentSourceFields;
  //
  private Label removeArgumentFieldsLabel;
  //
  private Button removeArgumentFieldsButton;
  //
  private TableView outputFieldsTableView;
  /**
   * The constructor should simply invoke super() and save the incoming meta
   * object to a local variable, so it can conveniently read and write settings
   * from/to it.
   * 
   * @param parent 	the SWT shell to open the dialog in
   * @param in the meta object holding the step's settings
   * @param transMeta	transformation description
   * @param sname		the step name
   */
  public JdbcMetaDataDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
    super(parent, (BaseStepMeta) in, transMeta, sname);
    meta = (JdbcMetaDataMeta) in;
  }

  private final String[] emptyFieldList = new String[0];
  private String[] getFieldListForCombo(){
    String[] items;
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      items = r.getFieldNames();
    }
    catch(KettleException exception) {
      items = emptyFieldList;
    }
    return items;
  }
  
  private void connectionSourceUpdated(){
    int selectedIndex = connectionSourceCombo.getSelectionIndex();
    String option = JdbcMetaDataMeta.connectionSourceOptions[selectedIndex];
    boolean connectionComboEnabled, connectionFieldEnabled, otherFieldsEnabled;
    otherFieldsEnabled = connectionComboEnabled = connectionFieldEnabled = false;
    String[] fields = emptyFieldList;
    if (JdbcMetaDataMeta.connectionSourceOptionConnection.equals(option)) {
      connectionComboEnabled = true;
    }
    else {
      if (JdbcMetaDataMeta.connectionSourceOptionConnectionField.equals(option)){
        connectionFieldEnabled = true;
        connectionField.setItems(getFieldListForCombo());
      }
      else {
        otherFieldsEnabled = true;
        if (JdbcMetaDataMeta.connectionSourceOptionJDBCFields.equals(option)){
          fields = getFieldListForCombo();
        }
      }
    }
    connectionCombo.setEnabled(connectionComboEnabled);
    connectionField.setEnabled(connectionFieldEnabled);
    jdbcDriverField.setEnabled(otherFieldsEnabled);
    jdbcDriverField.setItems(fields);
    jdbcUrlField.setEnabled(otherFieldsEnabled);
    jdbcUrlField.setItems(fields);
    jdbcUserField.setEnabled(otherFieldsEnabled);
    jdbcUserField.setItems(fields);
    jdbcPasswordField.setEnabled(otherFieldsEnabled);
    jdbcPasswordField.setItems(fields);
  }

  /**
   * Remove the UI to enter method arguments
   * The current values are stored and returned.
   */
  private List<String> removeArgumentsUI(){
    Control [] controls = metadataComposite.getChildren();
    List<String> currentValues = new ArrayList<String>();
    for (Control control : controls) {
      if (
        control == alwaysPassInputRowLabel || control == alwaysPassInputRowButton ||
        control == methodLabel || control == methodCombo || 
        control == argumentSourceLabel || control == argumentSourceFields ||
        control == removeArgumentFieldsLabel || control == removeArgumentFieldsButton
      ) continue;
      if (control instanceof CCombo) {
        currentValues.add(((CCombo)control).getText());
      }
      control.dispose();
    }
    return currentValues;
  }
  
  /**
   * Create the UI to enter one argument.
   * @param argumentDescriptor
   * @param lastControl
   * @param items
   * @return The combobox where the user enters the argument.
   */
  private ComboVar createArgumentUI(Object[] argumentDescriptor, Control lastControl, String[] items){
    String argumentName = (String)argumentDescriptor[0];
    Label label = new Label(metadataComposite, SWT.RIGHT);
    label.setText(BaseMessages.getString(PKG, "JdbcMetadata.arguments." + argumentName + ".Label"));
    label.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.arguments." + argumentName + ".Tooltip"));
    props.setLook(label);
    FormData labelFormData = new FormData();
    labelFormData.left = new FormAttachment(0, 0);
    labelFormData.right = new FormAttachment(middle, -margin);
    labelFormData.top = new FormAttachment(lastControl, margin);
    label.setLayoutData(labelFormData);
    
    ComboVar comboVar = new ComboVar(transMeta, metadataComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(comboVar);
    FormData comboVarFormData = new FormData();
    comboVarFormData.left = new FormAttachment(middle, 0);
    comboVarFormData.right = new FormAttachment(100, 0);
    comboVarFormData.top = new FormAttachment(lastControl, margin);
    comboVar.setLayoutData(comboVarFormData);
    comboVar.setItems(items);

    comboVar.addModifyListener(lsMod);
    
    return comboVar;
  }
  /**
   * Create UI to enter arguments.
   * Return a new set of arguments to store in the meta object
   */
  private String[] createArgumentsUI(Object[] argumentDescriptors, String[] currentValues){
    logDebug("createArgumentsUI, currentValues = " + (currentValues == null ? "null" : currentValues.length));
    Object[] argumentDescriptor;
    int argc = argumentDescriptors.length;
    String[] newArguments = new String[argc];
    Control lastControl = removeArgumentFieldsButton;
    String[] items = argumentSourceFields.getSelection() ? getFieldListForCombo() : emptyFieldList;
    for (int i = 0; i < argc; i++){
      argumentDescriptor = (Object[])argumentDescriptors[i];
      ComboVar comboVar = createArgumentUI(argumentDescriptor, lastControl, items);
      lastControl = comboVar;
      
      //copy the old argument values to the new arguments array
      if (i >= currentValues.length) continue;
      String argumentValue = currentValues[i];
      newArguments[i] = argumentValue;
      if (argumentValue == null) continue;
      comboVar.setText(argumentValue);
    }
    return newArguments;
  }
  
  /**
   * fill the fields table with output fields.
   */
  private void populateFieldsTable(Object[] methodDescriptor){
    logDebug("populateFieldsTable 1");
    Object[] outputFields = getOutputFields();
    outputFieldsTableView.clearAll();
    ValueMetaInterface[] fields = (ValueMetaInterface[])methodDescriptor[2];
    int n = fields.length;
    Table table = outputFieldsTableView.table;
    table.setItemCount(n);
    TableItem tableItem;
    String fieldName;
    ValueMetaInterface field;
    outputFieldsTableView.optWidth(true, n);
    int m = (outputFields == null) ? 0 : outputFields.length;
    String[] outputField;
    for (int i = 0; i < n; i++) {
      field = fields[i];
      tableItem = table.getItem(i);
      fieldName = field.getName();
      tableItem.setText(1, fieldName);
      //initially, the field is renamed unto itself.
      tableItem.setText(2, fieldName);
      //now see if the meta object renamed this field.
      for (int j = 0; j < m; j++) {
        outputField = (String[])outputFields[j];
        if (!fieldName.equals(outputField[0])) continue;
        tableItem.setText(2, outputField[1]);
        break;
      }
    }
  }

  private void populateFieldsTable(){
    logDebug("populateFieldsTable 2");
    populateFieldsTable(JdbcMetaDataMeta.getMethodDescriptor(methodCombo.getSelectionIndex()));
  }

  private void updateOutputFields(Object[] outputFields){
    logDebug("updateOutputFields " + outputFields);
    if (outputFields == null) return;
    outputFieldsTableView.clearAll();
    String[] outputField;
    int n = outputFields.length;
    Table table = outputFieldsTableView.table;
    table.setItemCount(n);
    TableItem tableItem;
    for (int i = 0; i < n; i++) {
      outputField = (String[])outputFields[i];
      tableItem = table.getItem(i);
      tableItem.setText(1, outputField[0]);
      tableItem.setText(2, outputField[1]);
    }
  }

  /**
   * When the method is updated, we need the ui to change to allow the user to enter arguments.
   * This takes care of that.
   */
  private void methodUpdated(String[] argumentValues){
    logDebug("methodUpdated, argumentValues = " + (argumentValues == null ? "null" : argumentValues.length));
    //first, remove the controls for the previous set of arguments
    List<String> currentValues = removeArgumentsUI();
    if (argumentValues == null) {
      argumentValues = new String[currentValues.size()];
      currentValues.toArray(argumentValues);
    }
    
    //setup controls for the current set of arguments
    int index = methodCombo.getSelectionIndex();
    Object[] methodDescriptor = (Object[])JdbcMetaDataMeta.methodDescriptors[index];
    Object[] argumentDescriptors = (Object[])methodDescriptor[1];

    String[] newArguments = createArgumentsUI(argumentDescriptors, argumentValues);
    //update the arguments in the meta object
    meta.setArguments(newArguments);
    //show / hide the argument source ui depending on whether we have arguments
    boolean visible = newArguments.length > 0;
    argumentSourceFields.setVisible(visible);
    argumentSourceLabel.setVisible(visible);
    removeArgumentFieldsLabel.setVisible(visible);
    removeArgumentFieldsButton.setVisible(visible);
    
    metadataComposite.layout();
  }
  
  private void methodUpdated(){
    logDebug("Parameterless methodUpdated called.");
    methodUpdated(null);
  }
  /**
   * This method is called by Spoon when the user opens the settings dialog of the step.
   * It should open the dialog and return only once the dialog has been closed by the user.
   * 
   * If the user confirms the dialog, the meta object (passed in the constructor) must
   * be updated to reflect the new step settings. The changed flag of the meta object must 
   * reflect whether the step configuration was changed by the dialog.
   * 
   * If the user cancels the dialog, the meta object must not be updated, and its changed flag
   * must remain unaltered.
   * 
   * The open() method must return the name of the step after the user has confirmed the dialog,
   * or null if the user cancelled the dialog.
   */
  public String open() {
    dialogChanged = false;
    // store some convenient SWT variables 
    Shell parent = getParent();
    Display display = parent.getDisplay();
    
    // SWT code for preparing the dialog
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, meta);
    
    // Save the value of the changed flag on the meta object. If the user cancels
    // the dialog, it will be restored to this saved value.
    // The "changed" variable is inherited from BaseStepDialog
    changed = meta.hasChanged();
    
    // The ModifyListener used on all controls. It will update the meta object to 
    // indicate that changes are being made.
    lsMod = new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        dialogChanged = true;
      }
    };
    
    // ------------------------------------------------------- //
    // SWT code for building the actual settings dialog        //
    // ------------------------------------------------------- //
    Control lastControl;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "JdbcMetaData.Shell.Title")); 

    // Stepname line
    wlStepname = new Label(shell, SWT.RIGHT);
    wlStepname.setText(BaseMessages.getString(PKG, "System.Label.StepName")); 
    props.setLook(wlStepname);
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment(0, 0);
    fdlStepname.right = new FormAttachment(middle, -margin);
    fdlStepname.top = new FormAttachment(0, margin);
    wlStepname.setLayoutData(fdlStepname);

    wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wStepname.setText(stepname);
    props.setLook(wStepname);
    wStepname.addModifyListener(lsMod);
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment(middle, 0);
    fdStepname.top = new FormAttachment(0, margin);
    fdStepname.right = new FormAttachment(100, 0);
    wStepname.setLayoutData(fdStepname);

    lastControl = wStepname;

    //Tabfolder
    CTabFolder cTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook(cTabFolder, Props.WIDGET_STYLE_TAB );

    //Connection tab
    CTabItem connectionTab = new CTabItem( cTabFolder, SWT.NONE );
    connectionTab.setText(BaseMessages.getString(PKG, "JdbcMetadata.ConnectionTab.Label"));
    connectionTab.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.ConnectionTab.Tooltip"));

    FormLayout connectionTabLayout = new FormLayout();
    connectionTabLayout.marginWidth = Const.FORM_MARGIN;
    connectionTabLayout.marginHeight = Const.FORM_MARGIN;

    Composite connectionComposite = new Composite( cTabFolder, SWT.NONE );
    props.setLook(connectionComposite);
    connectionComposite.setLayout(connectionTabLayout);

    //Connection source
    Label connectionSourceLabel = new Label(connectionComposite, SWT.RIGHT);
    connectionSourceLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.connectionSource.Label"));
    connectionSourceLabel.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.connectionSource.Tooltip"));
    props.setLook(connectionSourceLabel);
    FormData connectionSourceLabelFormData = new FormData();
    connectionSourceLabelFormData.left = new FormAttachment(0, 0);
    connectionSourceLabelFormData.right = new FormAttachment(middle, -margin);
    connectionSourceLabelFormData.top = new FormAttachment(lastControl, margin);
    connectionSourceLabel.setLayoutData(connectionSourceLabelFormData);

    connectionSourceCombo = new CCombo(connectionComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    connectionSourceCombo.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.connectionSource.Tooltip"));
    props.setLook(connectionSourceCombo);
    connectionSourceCombo.addModifyListener(lsMod);
    FormData connectionSourceComboFormData = new FormData();
    connectionSourceComboFormData.left = new FormAttachment(middle, 0);
    connectionSourceComboFormData.right = new FormAttachment(100, 0);
    connectionSourceComboFormData.top = new FormAttachment(lastControl, margin);
    connectionSourceCombo.setLayoutData(connectionSourceComboFormData);

    String[] connectionSourceOptions = new String[JdbcMetaDataMeta.connectionSourceOptions.length];
    for (int i = 0; i < JdbcMetaDataMeta.connectionSourceOptions.length; i++) {
      connectionSourceOptions[i] = BaseMessages.getString(
        PKG, "JdbcMetadata.connectionSource.options." + JdbcMetaDataMeta.connectionSourceOptions[i]
      );
    }
    connectionSourceCombo.setItems(connectionSourceOptions);
    connectionSourceCombo.setEditable(false);
    connectionSourceCombo.addSelectionListener(new SelectionListener(){
      @Override
      public void widgetDefaultSelected(SelectionEvent selectionEvent) {	
      }
      @Override
      public void widgetSelected(SelectionEvent selectionEvent) {
        connectionSourceUpdated();
      }
    });
    lastControl = connectionSourceCombo;

    // Connection line
    connectionCombo = addConnectionLine(connectionComposite, lastControl, middle, margin );
/*    
      if (meta.getDatabaseMeta() == null && transMeta.nrDatabases() == 1 ) {
        connectionCombo.select( 0 );
      }
*/
    connectionCombo.addModifyListener( lsMod );
    //connectionCombo.addSelectionListener( lsSelection );
    lastControl = connectionCombo;

    //connection name field
    Label connectionFieldLabel = new Label(connectionComposite, SWT.RIGHT);
    connectionFieldLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.connectionField.Label"));
    connectionFieldLabel.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.connectionField.Tooltip"));
    props.setLook(connectionFieldLabel);
    FormData connectionFieldLabelFormData = new FormData();
    connectionFieldLabelFormData.left = new FormAttachment(0, 0);
    connectionFieldLabelFormData.right = new FormAttachment(middle, -margin);
    connectionFieldLabelFormData.top = new FormAttachment(lastControl, margin);
    connectionFieldLabel.setLayoutData(connectionFieldLabelFormData);

    connectionField = new CCombo( connectionComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook(connectionField);
    connectionField.addModifyListener(lsMod);
    FormData connectionFieldFormData = new FormData();
    connectionFieldFormData.left = new FormAttachment(middle, 0);
    connectionFieldFormData.right = new FormAttachment(100, 0);
    connectionFieldFormData.top = new FormAttachment(lastControl, margin);
    connectionField.setLayoutData(connectionFieldFormData);

    lastControl = connectionField;

    //jdbc driver field
    Label jdbcDriverLabel = new Label(connectionComposite, SWT.RIGHT);
    jdbcDriverLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.driverField.Label")); 
    jdbcDriverLabel.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.driverField.Tooltip")); 
    props.setLook(jdbcDriverLabel);
    FormData jdbcDriverLabelFormData = new FormData();
    jdbcDriverLabelFormData.left = new FormAttachment(0, 0);
    jdbcDriverLabelFormData.right = new FormAttachment(middle, -margin);
    jdbcDriverLabelFormData.top = new FormAttachment(lastControl, margin);
    jdbcDriverLabel.setLayoutData(jdbcDriverLabelFormData);

    jdbcDriverField = new ComboVar( transMeta, connectionComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook(jdbcDriverField);
    jdbcDriverField.addModifyListener(lsMod);
    FormData jdbcDriverFieldFormData = new FormData();
    jdbcDriverFieldFormData.left = new FormAttachment(middle, 0);
    jdbcDriverFieldFormData.right = new FormAttachment(100, 0);
    jdbcDriverFieldFormData.top = new FormAttachment(lastControl, margin);
    jdbcDriverField.setLayoutData(jdbcDriverFieldFormData);

    lastControl = jdbcDriverField;

    //jdbc url field
    Label jdbcUrlLabel = new Label(connectionComposite, SWT.RIGHT);
    jdbcUrlLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.urlField.Label")); 
    jdbcUrlLabel.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.urlField.Tooltip")); 
    props.setLook(jdbcUrlLabel);
    FormData jdbcUrlLabelFormData = new FormData();
    jdbcUrlLabelFormData.left = new FormAttachment(0, 0);
    jdbcUrlLabelFormData.right = new FormAttachment(middle, -margin);
    jdbcUrlLabelFormData.top = new FormAttachment(lastControl, margin);
    jdbcUrlLabel.setLayoutData(jdbcUrlLabelFormData);

    jdbcUrlField = new ComboVar(transMeta, connectionComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(jdbcUrlField);
    jdbcUrlField.addModifyListener(lsMod);
    FormData jdbcUrlFieldFormData = new FormData();
    jdbcUrlFieldFormData.left = new FormAttachment(middle, 0);
    jdbcUrlFieldFormData.right = new FormAttachment(100, 0);
    jdbcUrlFieldFormData.top = new FormAttachment(lastControl, margin);
    jdbcUrlField.setLayoutData(jdbcUrlFieldFormData);

    lastControl = jdbcUrlField;

    //jdbc user field
    Label jdbcUserLabel = new Label(connectionComposite, SWT.RIGHT);
    jdbcUserLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.userField.Label")); 
    jdbcUserLabel.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.userField.Tooltip")); 
    props.setLook(jdbcUserLabel);
    FormData jdbcUserLabelFormData = new FormData();
    jdbcUserLabelFormData.left = new FormAttachment(0, 0);
    jdbcUserLabelFormData.right = new FormAttachment(middle, -margin);
    jdbcUserLabelFormData.top = new FormAttachment(lastControl, margin);
    jdbcUserLabel.setLayoutData(jdbcUserLabelFormData);

    jdbcUserField = new ComboVar(transMeta, connectionComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(jdbcUserField);
    jdbcUserField.addModifyListener(lsMod);
    FormData jdbcUserFieldFormData = new FormData();
    jdbcUserFieldFormData.left = new FormAttachment(middle, 0);
    jdbcUserFieldFormData.right = new FormAttachment(100, 0);
    jdbcUserFieldFormData.top = new FormAttachment(lastControl, margin);
    jdbcUserField.setLayoutData(jdbcUserFieldFormData);

    lastControl = jdbcUserField;

    //jdbc password field
    Label jdbcPasswordLabel = new Label(connectionComposite, SWT.RIGHT);
    jdbcPasswordLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.passwordField.Label")); 
    jdbcPasswordLabel.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.passwordField.Tooltip")); 
    props.setLook(jdbcPasswordLabel);
    FormData jdbcPasswordLabelFormData = new FormData();
    jdbcPasswordLabelFormData.left = new FormAttachment(0, 0);
    jdbcPasswordLabelFormData.right = new FormAttachment(middle, -margin);
    jdbcPasswordLabelFormData.top = new FormAttachment(lastControl, margin);
    jdbcPasswordLabel.setLayoutData(jdbcPasswordLabelFormData);

    jdbcPasswordField = new ComboVar(transMeta, connectionComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(jdbcPasswordField);
    jdbcPasswordField.addModifyListener(lsMod);
    FormData jdbcPasswordFieldFormData = new FormData();
    jdbcPasswordFieldFormData.left = new FormAttachment(middle, 0);
    jdbcPasswordFieldFormData.right = new FormAttachment(100, 0);
    jdbcPasswordFieldFormData.top = new FormAttachment(lastControl, margin);
    jdbcPasswordField.setLayoutData(jdbcPasswordFieldFormData);

    lastControl = jdbcPasswordField;

    //layout the connection tab
    FormData connectionTabFormData = new FormData();
    connectionTabFormData.left = new FormAttachment( 0, 0 );
    connectionTabFormData.top = new FormAttachment( 0, 0 );
    connectionTabFormData.right = new FormAttachment( 100, 0 );
    connectionTabFormData.bottom = new FormAttachment( 100, 0 );
    connectionComposite.setLayoutData(connectionTabFormData);
    connectionComposite.layout();
    connectionTab.setControl(connectionComposite);

    //Metadata tab
    CTabItem metadataTab = new CTabItem( cTabFolder, SWT.NONE );
    metadataTab.setText(BaseMessages.getString( PKG, "JdbcMetadata.MetaDataTab.Label" ) );
    metadataTab.setToolTipText(BaseMessages.getString( PKG, "JdbcMetadata.MetaDataTab.Tooltip"));
    
    FormLayout metadataTabLayout = new FormLayout();
    connectionTabLayout.marginWidth = Const.FORM_MARGIN;
    connectionTabLayout.marginHeight = Const.FORM_MARGIN;

    metadataComposite = new Composite( cTabFolder, SWT.NONE );
    props.setLook(metadataComposite);
    metadataComposite.setLayout(metadataTabLayout);

    //pass the row checkbox
    alwaysPassInputRowLabel = new Label(metadataComposite, SWT.RIGHT);
    alwaysPassInputRowLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.passRow.Label"));
    alwaysPassInputRowLabel.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.passRow.Tooltip"));
    props.setLook(alwaysPassInputRowLabel);
    FormData alwaysPassInputRowLabelFormData = new FormData();
    alwaysPassInputRowLabelFormData.left = new FormAttachment(0, 0);
    alwaysPassInputRowLabelFormData.right = new FormAttachment(middle, -margin);
    alwaysPassInputRowLabelFormData.top = new FormAttachment(lastControl, margin);
    alwaysPassInputRowLabel.setLayoutData(alwaysPassInputRowLabelFormData);

    alwaysPassInputRowButton = new Button(metadataComposite, SWT.CHECK);
    props.setLook(alwaysPassInputRowButton);
    FormData alwaysPassInputRowButtonFormData = new FormData();
    alwaysPassInputRowButtonFormData.left = new FormAttachment(middle, 0);
    alwaysPassInputRowButtonFormData.right = new FormAttachment(100, 0);
    alwaysPassInputRowButtonFormData.top = new FormAttachment(lastControl, margin);
    alwaysPassInputRowButton.setLayoutData(alwaysPassInputRowButtonFormData);
    
    lastControl = alwaysPassInputRowButton;

    //method
    methodLabel = new Label(metadataComposite, SWT.RIGHT);
    methodLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.metadataMethod.Label"));
    methodLabel.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.metadataMethod.Tooltip"));
    props.setLook(methodLabel );
    FormData methodLabelFormData = new FormData();
    methodLabelFormData.left = new FormAttachment(0, 0);
    methodLabelFormData.right = new FormAttachment(middle, -margin);
    methodLabelFormData.top = new FormAttachment(lastControl, margin);
    methodLabel.setLayoutData(methodLabelFormData);

    methodCombo = new CCombo(metadataComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(methodCombo);
    methodCombo.setEditable(false);
    methodCombo.addModifyListener(lsMod);
    FormData methodComboFormData = new FormData();
    methodComboFormData.left = new FormAttachment(middle, 0);
    methodComboFormData.right = new FormAttachment(100, 0);
    methodComboFormData.top = new FormAttachment(lastControl, margin);
    methodCombo.setLayoutData(methodComboFormData);

    Object[] methodDescriptor;
    String methodName;
    for (int i = 0; i < JdbcMetaDataMeta.methodDescriptors.length; i++){
      methodDescriptor = (Object[])JdbcMetaDataMeta.methodDescriptors[i];
      methodName = (String)methodDescriptor[0];
      methodCombo.add(BaseMessages.getString(PKG, "JdbcMetadata.methods." + methodName));
    }
    
    SelectionListener methodComboSelectionListener = new SelectionListener(){
      @Override
      public void widgetDefaultSelected(SelectionEvent selectionEvent) {
      }

      @Override
      public void widgetSelected(SelectionEvent selectionEvent) {
        logDebug("methodCombo changed, calling parameterless methodUpdated");
        methodUpdated();
        populateFieldsTable();
      }
      
    };
    methodCombo.addSelectionListener(methodComboSelectionListener);
    lastControl = methodCombo;

    //argument source
    argumentSourceLabel = new Label(metadataComposite, SWT.RIGHT);
    argumentSourceLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.argumentSource.Label"));
    argumentSourceLabel.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.argumentSource.Tooltip"));
    props.setLook(argumentSourceLabel);
    FormData argumentSourceLabelFormData = new FormData();
    argumentSourceLabelFormData.left = new FormAttachment(0, 0);
    argumentSourceLabelFormData.right = new FormAttachment(middle, -margin);
    argumentSourceLabelFormData.top = new FormAttachment(lastControl, margin);
    argumentSourceLabel.setLayoutData(argumentSourceLabelFormData);

    argumentSourceFields = new Button(metadataComposite, SWT.CHECK);
    props.setLook(argumentSourceFields);
    FormData argumentSourceFieldsFormData = new FormData();
    argumentSourceFieldsFormData.left = new FormAttachment(middle, 0);
    argumentSourceFieldsFormData.right = new FormAttachment(100, 0);
    argumentSourceFieldsFormData.top = new FormAttachment(lastControl, margin);
    argumentSourceFields.setLayoutData(argumentSourceFieldsFormData);
    SelectionListener argumentSourceFieldsSelectionListener = new SelectionListener(){
      @Override
      public void widgetDefaultSelected(SelectionEvent selectionEvent) {
      }

      @Override
      public void widgetSelected(SelectionEvent selectionEvent) {
        Control [] controls = metadataComposite.getChildren();
        boolean selection = argumentSourceFields.getSelection();
        removeArgumentFieldsButton.setEnabled(selection);
        String[] items = selection ? getFieldListForCombo() : emptyFieldList;
        for (Control control : controls) {
          if (!(control instanceof CCombo) || control == methodCombo) continue;
          CCombo cCombo = (CCombo)control;
          cCombo.setItems(items);
        }
      }
      
    };
    argumentSourceFields.addSelectionListener(argumentSourceFieldsSelectionListener);

    lastControl = argumentSourceFields;
    
    //argument source
    removeArgumentFieldsLabel = new Label(metadataComposite, SWT.RIGHT);
    removeArgumentFieldsLabel.setText(BaseMessages.getString(PKG, "JdbcMetadata.removeArgumentFields.Label"));
    removeArgumentFieldsLabel.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.removeArgumentFields.Tooltip"));
    props.setLook(removeArgumentFieldsLabel);
    FormData removeArgumentFieldsLabelFormData = new FormData();
    removeArgumentFieldsLabelFormData.left = new FormAttachment(0, 0);
    removeArgumentFieldsLabelFormData.right = new FormAttachment(middle, -margin);
    removeArgumentFieldsLabelFormData.top = new FormAttachment(lastControl, margin);
    removeArgumentFieldsLabel.setLayoutData(removeArgumentFieldsLabelFormData);

    removeArgumentFieldsButton = new Button(metadataComposite, SWT.CHECK);
    props.setLook(removeArgumentFieldsButton);
    FormData removeArgumentFieldsButtonFormData = new FormData();
    removeArgumentFieldsButtonFormData.left = new FormAttachment(middle, 0);
    removeArgumentFieldsButtonFormData.right = new FormAttachment(100, 0);
    removeArgumentFieldsButtonFormData.top = new FormAttachment(lastControl, margin);
    removeArgumentFieldsButton.setLayoutData(removeArgumentFieldsButtonFormData);

    //layout the metdata tab
    FormData metadataTabFormData = new FormData();
    metadataTabFormData.left = new FormAttachment( 0, 0 );
    metadataTabFormData.top = new FormAttachment( 0, 0 );
    metadataTabFormData.right = new FormAttachment( 100, 0 );
    metadataTabFormData.bottom = new FormAttachment( 100, 0 );
    metadataComposite.setLayoutData(metadataTabFormData);
    metadataComposite.layout();
    metadataTab.setControl(metadataComposite);

    //Fields tab
    CTabItem fieldsTab = new CTabItem( cTabFolder, SWT.NONE );
    fieldsTab.setText( BaseMessages.getString( PKG, "JdbcMetadata.FieldsTab.Label"));
    fieldsTab.setToolTipText(BaseMessages.getString(PKG, "JdbcMetadata.FieldsTab.Tooltip"));

    FormLayout fieldsTabLayout = new FormLayout();
    fieldsTabLayout.marginWidth = Const.FORM_MARGIN;
    fieldsTabLayout.marginHeight = Const.FORM_MARGIN;

    Composite fieldsComposite = new Composite( cTabFolder, SWT.NONE );
    props.setLook(fieldsComposite);
    fieldsComposite.setLayout(fieldsTabLayout);

    //add UI for the fields tab.
    Label outputFieldsTableViewLabel = new Label(fieldsComposite, SWT.NONE );
    outputFieldsTableViewLabel.setText(BaseMessages.getString( PKG, "JdbcMetadata.FieldsTab.Label" ) );
    outputFieldsTableViewLabel.setToolTipText(BaseMessages.getString( PKG, "JdbcMetadata.FieldsTab.Tooltip" ) );
    props.setLook(outputFieldsTableViewLabel);
    FormData outputFieldsTableViewLabelFormData = new FormData();
    outputFieldsTableViewLabelFormData.left = new FormAttachment( 0, 0 );
    outputFieldsTableViewLabelFormData.top = new FormAttachment( 0, margin );
    outputFieldsTableViewLabel.setLayoutData(outputFieldsTableViewLabelFormData);
    
    ColumnInfo[] columnInfo = new ColumnInfo[]{
      new ColumnInfo(
        BaseMessages.getString(PKG, "JdbcMetadata.FieldName.Label"),
        ColumnInfo.COLUMN_TYPE_NONE
      ),
      new ColumnInfo(
        BaseMessages.getString(PKG, "JdbcMetadata.OutputFieldName.Label"),
        ColumnInfo.COLUMN_TYPE_TEXT
      )
    };
    outputFieldsTableView = new TableView(
      transMeta, 
      fieldsComposite, 
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
      columnInfo, 10, lsMod, props
    );

    Button getFieldsButton = new Button( fieldsComposite, SWT.PUSH );
    getFieldsButton.setText( BaseMessages.getString( PKG, "JdbcMetadata.getFieldsButton.Label" ) );
    getFieldsButton.setToolTipText(BaseMessages.getString( PKG, "JdbcMetadata.getFieldsButton.Tooltip" ) );
    FormData getFieldsButtonFormData = new FormData();
    getFieldsButtonFormData.top = new FormAttachment(outputFieldsTableViewLabel, margin );
    getFieldsButtonFormData.right = new FormAttachment( 100, 0 );
    getFieldsButton.setLayoutData(getFieldsButtonFormData);
    getFieldsButton.addSelectionListener(new SelectionListener(){

      @Override
      public void widgetDefaultSelected(SelectionEvent arg0) {
      }

      @Override
      public void widgetSelected(SelectionEvent arg0) {
        populateFieldsTable();
      }
      
    });

    FormData outputFieldsTableViewFormData = new FormData();
    outputFieldsTableViewFormData.left = new FormAttachment( 0, 0 );
    outputFieldsTableViewFormData.top = new FormAttachment(outputFieldsTableViewLabel, margin );
    outputFieldsTableViewFormData.right = new FormAttachment(getFieldsButton, -margin );
    outputFieldsTableViewFormData.bottom = new FormAttachment( 100, -2*margin );
    outputFieldsTableView.setLayoutData(outputFieldsTableViewFormData);
    
    //layout the fields tab
    FormData fieldsTabFormData = new FormData();
    fieldsTabFormData.left = new FormAttachment( 0, 0 );
    fieldsTabFormData.top = new FormAttachment( 0, 0 );
    fieldsTabFormData.right = new FormAttachment( 100, 0 );
    fieldsTabFormData.bottom = new FormAttachment( 100, 0 );
    fieldsComposite.setLayoutData(metadataTabFormData);
    fieldsComposite.layout();
    fieldsTab.setControl(fieldsComposite);

    // OK and cancel buttons
    wOK = new Button(shell, SWT.PUSH);
    wOK.setText(BaseMessages.getString(PKG, "System.Button.OK")); 
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); 

    BaseStepDialog.positionBottomButtons(shell, new Button[] { wOK, wCancel }, margin, null);

    FormData cTabFolderFormData = new FormData();
    cTabFolderFormData.left = new FormAttachment( 0, 0 );
    cTabFolderFormData.top = new FormAttachment(wStepname, margin );
    cTabFolderFormData.right = new FormAttachment( 100, 0 );
    cTabFolderFormData.bottom = new FormAttachment( wOK, -margin );
    cTabFolder.setLayoutData(cTabFolderFormData);	    
    cTabFolder.setSelection( 0 );

    // Add listeners for cancel and OK
    lsCancel = new Listener() {
      public void handleEvent(Event e) {cancel();}
    };
    lsOK = new Listener() {
      public void handleEvent(Event e) {ok();}
    };

    wCancel.addListener(SWT.Selection, lsCancel);
    wOK.addListener(SWT.Selection, lsOK);

    // default listener (for hitting "enter")
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };
    wStepname.addSelectionListener(lsDef);
    jdbcDriverField.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window and cancel the dialog properly
    shell.addShellListener(new ShellAdapter() {
      public void shellClosed(ShellEvent e) {cancel();}
    });

    // Set/Restore the dialog size based on last position on screen
    // The setSize() method is inherited from BaseStepDialog
    setSize();

    // populate the dialog with the values from the meta object
    populateDialog();

    // restore the changed flag to original value, as the modify listeners fire during dialog population 
    meta.setChanged(changed);

    // open dialog and enter event loop 
    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) display.sleep();
    }

    // at this point the dialog has closed, so either ok() or cancel() have been executed
    // The "stepname" variable is inherited from BaseStepDialog
    return stepname;
  }

  private void selectConnectionSource(String connectionSourceOption){
    int index = JdbcMetaDataMeta.getConnectionSourceOptionIndex(connectionSourceOption);
    connectionSourceCombo.select(index);
    connectionSourceUpdated();
  }
  
  private void setMethod(String method) {
    int index = JdbcMetaDataMeta.getMethodDescriptorIndex(method);
    if (index == -1) throw new IllegalArgumentException("Index for method " + method + " is -1.");
    methodCombo.select(index);
    logDebug("setMethod called, calling parameterless methodupdated");
  }
  
  /**
   * This helper method puts the step configuration stored in the meta object
   * and puts it into the dialog controls.
   */
  private void populateDialog() {
    wStepname.selectAll();
    String value;

    value = meta.getConnectionSource();
    selectConnectionSource(value);

    value = meta.getConnectionName();
    if (value != null) connectionCombo.setText(value);

    value = meta.getConnectionField();
    if (value != null) connectionField.setText(value);

    value = meta.getJdbcDriverField();
    if (value != null) jdbcDriverField.setText(value);

    value = meta.getJdbcUrlField();
    if (value != null) jdbcUrlField.setText(value);	

    value = meta.getJdbcUserField();
    if (value != null) jdbcUserField.setText(value);

    value = meta.getJdbcPasswordField();
    if (value != null) jdbcPasswordField.setText(value);

    alwaysPassInputRowButton.setSelection(meta.getAlwaysPassInputRow());

    value = meta.getMethodName();
    if (value != null) setMethod(value);
    
    argumentSourceFields.setSelection(meta.getArgumentSourceFields());
    methodUpdated(meta.getArguments());

    logDebug("Calling methodUpdated from populate dialog.");
    updateOutputFields(meta.getOutputFields());
    removeArgumentFieldsButton.setSelection(meta.getRemoveArgumentFields());
    removeArgumentFieldsButton.setEnabled(meta.getArgumentSourceFields());
  }

  /**
   * Called when the user cancels the dialog.  
   */
  private void cancel() {
    // The "stepname" variable will be the return value for the open() method. 
    // Setting to null to indicate that dialog was cancelled.
    stepname = null;
    // Restoring original "changed" flag on the met aobject
    meta.setChanged(changed);
    // close the SWT dialog window
    dispose();
  }
  
  /**
   * Called when the user confirms the dialog
   */
  private void ok() {
    // The "stepname" variable will be the return value for the open() method. 
    // Setting to step name from the dialog control
    stepname = wStepname.getText(); 
    // Save settings to the meta object
    meta.setConnectionSource(JdbcMetaDataMeta.connectionSourceOptions[connectionSourceCombo.getSelectionIndex()]);
    meta.setConnectionName(connectionCombo.getText());
    meta.setConnectionField(connectionField.getText());
    meta.setJdbcDriverField(jdbcDriverField.getText());
    meta.setJdbcUrlField(jdbcUrlField.getText());
    meta.setJdbcUserField(jdbcUserField.getText());
    meta.setJdbcPasswordField(jdbcPasswordField.getText());
    meta.setAlwaysPassInputRow(alwaysPassInputRowButton.getSelection());
    meta.setMethodName(JdbcMetaDataMeta.getMethodName(methodCombo.getSelectionIndex()));
    meta.setArgumentSourceFields(argumentSourceFields.getSelection());
    meta.setArguments(getArguments());
    meta.setRemoveArgumentFields(removeArgumentFieldsButton.getSelection());
    meta.setOutputFields(getOutputFields());

    meta.setChanged(dialogChanged || changed);
    // close the SWT dialog window
    dispose();
  }

  private String[] getArguments(){
    logDebug("getArguments");
    List<String> list = new ArrayList<String>();
    Control [] controls = metadataComposite.getChildren();
    String text;
    for (Control control : controls) {
      if (!(control instanceof ComboVar)) continue;
      ComboVar comboVar = (ComboVar)control;
      text = comboVar.getText();
      list.add(text);
    }
    String[] arguments = new String[list.size()];
    list.toArray(arguments);
    return arguments;
  }

  private Object[] getOutputFields(){
    Table table = outputFieldsTableView.table;
    int n = table.getItemCount();
    Object[] outputFields = new Object[n];
    String[] outputField;
    TableItem tableItem;
    for (int i = 0; i < n; i++) {
      tableItem = table.getItem(i);
      outputField = new String[]{
        tableItem.getText(1),
        tableItem.getText(2)
      };
      outputFields[i] = outputField;
    }
    return outputFields;
  }
}
