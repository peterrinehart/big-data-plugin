package org.pentaho.big.data.kettle.plugins.formats.impl.waterline;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.big.data.kettle.plugins.formats.impl.NullableValuesEnum;
import org.pentaho.big.data.kettle.plugins.formats.impl.parquet.BaseParquetStepDialog;
import org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output.ParquetOutputDialog;
import org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output.ParquetOutputMeta;
import org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileOutputDialog;
import org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileOutputMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.FormDataBuilder;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ColumnsResizer;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;

public class WaterlineCatalogWriterDialog extends HadoopFileOutputDialog {

  private ParquetOutputDialog parquetOutputDialog;
  private WaterlineCatalogWriterMeta meta;
  private TableView wParquetOutputFields;

  public WaterlineCatalogWriterDialog( Shell parent, Object in,
                                       TransMeta transMeta, String sname ) {
    super( parent, in, transMeta, sname );
    parquetOutputDialog = new ParquetOutputDialog( parent,  ((WaterlineCatalogWriterMeta) in).getParquetOutputMeta(), transMeta, sname );
    meta = (WaterlineCatalogWriterMeta) in;
  }

  @Override
  public void hack() {
    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        meta.setChanged();
      }
    };

    String[] typeNames = new String[ parquetOutputDialog.SUPPORTED_PARQUET_TYPES.length ];
    for ( int i = 0; i < typeNames.length; i++ ) {
      typeNames[ i ] = parquetOutputDialog.SUPPORTED_PARQUET_TYPES[ i ].getName();
    }
    ColumnInfo[] parameterColumns = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( ParquetOutputMeta.class, "ParquetOutputDialog.Fields.column.Path" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( ParquetOutputMeta.class, "ParquetOutputDialog.Fields.column.Name" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( ParquetOutputMeta.class, "ParquetOutputDialog.Fields.column.Type" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, typeNames, false ),
      new ColumnInfo( BaseMessages.getString( ParquetOutputMeta.class, "ParquetOutputDialog.Fields.column.Precision" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( ParquetOutputMeta.class, "ParquetOutputDialog.Fields.column.Scale" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( ParquetOutputMeta.class, "ParquetOutputDialog.Fields.column.Default" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( ParquetOutputMeta.class, "ParquetOutputDialog.Fields.column.Null" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, NullableValuesEnum.getValuesArr(), true ) };
    parameterColumns[ 0 ].setAutoResize( false );
    parameterColumns[ 1 ].setUsingVariables( true );
    wParquetOutputFields =
      new TableView( transMeta, wFields.getParent(),
        SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER | SWT.NO_SCROLL | SWT.V_SCROLL,
        parameterColumns, 7, lsMod, props );
    ColumnsResizer resizer = new ColumnsResizer( 0, 30, 20, 10, 10, 10, 15, 5 );
    wParquetOutputFields.getTable().addListener( SWT.Resize, resizer );


    props.setLook( wParquetOutputFields );
    new FormDataBuilder().left( 0, 0 ).right( 100, 0 ).top( wFields.getParent(), 0 ).result();

    wExtension.addModifyListener( modifyEvent -> toggleFields() );

    toggleFields();
  }

  private void toggleFields() {
    if ( wExtension.getText().compareToIgnoreCase( "pqt" ) == 0 ) {
      wFields.setVisible( false );
      wParquetOutputFields.setVisible( true );
    } else {
      wFields.setVisible( true );
      wParquetOutputFields.setVisible( false );
    }
  }

  @Override
  protected void get() {
    if ( wExtension.getText().compareToIgnoreCase( "pqt" ) == 0 ) {
      try {
        RowMetaInterface r = transMeta.getPrevStepFields( stepname );
        TableItemInsertListener listener = ( tableItem, v ) -> true;
        parquetOutputDialog.getFieldsFromPreviousStep( r, wParquetOutputFields, 1, new int[] { 1, 2 }, new int[] { 3 }, 4, 5, true, listener );

        // fix empty null fields to nullable
        for ( int i = 0; i < wParquetOutputFields.table.getItemCount(); i++ ) {
          TableItem tableItem = wParquetOutputFields.table.getItem( i );
          if ( StringUtils.isEmpty( tableItem.getText( 7 ) ) ) {
            tableItem.setText( 7, "Yes" );
          }
        }

        meta.setChanged();
      } catch ( KettleException ke ) {
        new ErrorDialog( shell, BaseMessages.getString( ParquetOutputMeta.class, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
          .getString( ParquetOutputMeta.class, "System.Dialog.GetFieldsFailed.Message" ), ke );
      }

    } else {
      super.get();
    }
  }

  @Override
  public void getData() {
    super.getData();
    parquetOutputDialog.populateFieldsUI( meta.getParquetOutputMeta(), wParquetOutputFields );
  }

  @Override
  public void getInfo( HadoopFileOutputMeta meta ) {
    super.getInfo( meta );
    parquetOutputDialog.saveOutputFields( wParquetOutputFields, ((WaterlineCatalogWriterMeta) meta).getParquetOutputMeta() );
    ((WaterlineCatalogWriterMeta) meta).getParquetOutputMeta().setFilename( wFilename.getText() );
  }

}
