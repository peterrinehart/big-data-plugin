package org.pentaho.big.data.kettle.plugins.formats.impl.waterline;

import org.pentaho.big.data.kettle.plugins.formats.impl.NamedClusterResolver;
import org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output.ParquetOutputData;
import org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output.ParquetOutputMeta;
import org.pentaho.big.data.kettle.plugins.formats.parquet.output.ParquetOutputField;
import org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileOutputMeta;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutput;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutputData;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@Step( id = "WaterlineCatalogWriter", image = "WaterlineCatalogWriter.svg", name = "Waterline Catalog Writer",
  description = "Write file to the catalog", categoryDescription = "Big Data" )
@InjectionSupported( localizationPrefix = "WaterlineCatalogWriterMeta.Injection." )
public class WaterlineCatalogWriterMeta extends HadoopFileOutputMeta {

  private NamedClusterResolver namedClusterResolver;
  private ParquetOutputMeta parquetOutputMeta;

  public WaterlineCatalogWriterMeta() {
    this( null, null, null, null );
    this.setParquetOutputMeta( new ParquetOutputMeta( null ) );
  }

  public WaterlineCatalogWriterMeta( NamedClusterService namedClusterService,
                                     RuntimeTestActionService runtimeTestActionService,
                                     RuntimeTester runtimeTester ) {
    super( namedClusterService, runtimeTestActionService, runtimeTester );
    this.setParquetOutputMeta( new ParquetOutputMeta( null ) );
    this.setNamedClusterResolver( null );
  }

  public WaterlineCatalogWriterMeta( NamedClusterResolver namedClusterResolver,
                                     NamedClusterService namedClusterService,
                                     RuntimeTestActionService runtimeTestActionService,
                                     RuntimeTester runtimeTester ) {
    super( namedClusterService, runtimeTestActionService, runtimeTester );
    this.setNamedClusterResolver( namedClusterResolver );
    setParquetOutputMeta( new ParquetOutputMeta( namedClusterResolver ) );
  }

  public NamedClusterResolver getNamedClusterResolver() {
    return namedClusterResolver;
  }

  public void setNamedClusterResolver(
    NamedClusterResolver namedClusterResolver ) {
    this.namedClusterResolver = namedClusterResolver;
  }

  public ParquetOutputMeta getParquetOutputMeta() {
    //this.parquetOutputMeta.
    return parquetOutputMeta;
  }

  public void setParquetOutputMeta(
    ParquetOutputMeta parquetOutputMeta ) {
    this.parquetOutputMeta = parquetOutputMeta;
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
                                Trans trans ) {
    if ( this.getExtension().compareToIgnoreCase( "pqt" ) == 0 ) {
      return new WaterlineParquetOutput( stepMeta, stepDataInterface, cnr, transMeta, trans );
    } else {
      return new TextFileOutput( stepMeta, stepDataInterface, cnr, transMeta, trans );
    }
  }

  @Override
  public StepDataInterface getStepData() {
    if ( this.getExtension().compareToIgnoreCase( "pqt" ) == 0 ) {
      return new ParquetOutputData();
    } else {
      return super.getStepData();
    }
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    super.loadXML( stepnode, databases, metaStore );
    try {
      Node fields = XMLHandler.getSubNode( stepnode, "parquet_fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );
      List<ParquetOutputField> parquetOutputFields = new ArrayList<>();
      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        ParquetOutputField outputField = new ParquetOutputField();
        outputField.setFormatFieldName( XMLHandler.getTagValue( fnode, "parquet_path" ) );
        outputField.setPentahoFieldName( XMLHandler.getTagValue( fnode, "parquet_name" ) );
        int parquetTypeId = this.parquetOutputMeta.getParquetTypeId( XMLHandler.getTagValue( fnode, "parquet_type" ) );
        outputField.setFormatType( parquetTypeId );
        outputField.setPrecision( XMLHandler.getTagValue( fnode, "parquet_precision" ) );
        outputField.setScale( XMLHandler.getTagValue( fnode, "parquet_scale" ) );
        outputField.setAllowNull( "Y".equalsIgnoreCase( XMLHandler.getTagValue( fnode, "parquet_nullable" ) ) );
        outputField.setDefaultValue( XMLHandler.getTagValue( fnode, "parquet_default" ) );
        parquetOutputFields.add( outputField );
      }
      this.parquetOutputMeta.setOutputFields( parquetOutputFields );
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load step info from XML", e );
    }
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer( 800 );
    retval.append( super.getXML() );

    retval.append( "    <parquet_fields>" ).append( Const.CR );
    for ( int i = 0; i < this.parquetOutputMeta.getOutputFields().size(); i++ ) {
      ParquetOutputField field = this.parquetOutputMeta.getOutputFields().get( i );

      if ( field.getPentahoFieldName() != null && field.getPentahoFieldName().length() != 0 ) {
        retval.append( "      <field>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( "parquet_path", field.getFormatFieldName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "parquet_name", field.getPentahoFieldName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "parquet_type", field.getFormatType() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "parquet_precision", field.getPrecision() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "parquet_scale", field.getScale() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "parquet_nullable", field.getAllowNull() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "parquet_default", field.getDefaultValue() ) );
        retval.append( "      </field>" ).append( Const.CR );
      }
    }
    retval.append( "    </parquet_fields>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    super.readRep( rep, metaStore, id_step, databases );

    try {
      // using the "type" column to get the number of field rows because "type" is guaranteed not to be null.
      int nrfields = rep.countNrStepAttributes( id_step, "parquet_type" );

      List<ParquetOutputField> parquetOutputFields = new ArrayList<>();
      for ( int i = 0; i < nrfields; i++ ) {
        ParquetOutputField outputField = new ParquetOutputField();
        outputField.setFormatFieldName( rep.getStepAttributeString( id_step, i, "parquet_path" ) );
        outputField.setPentahoFieldName( rep.getStepAttributeString( id_step, i, "parquet_name" ) );
        int parquetTypeId = this.parquetOutputMeta.getParquetTypeId( rep.getStepAttributeString( id_step, i, "parquet_type" ) );
        outputField.setFormatType( parquetTypeId );
        outputField.setPrecision( rep.getStepAttributeString( id_step, i, "parquet_precision" ) );
        outputField.setScale( rep.getStepAttributeString( id_step, i, "parquet_scale" ) );
        outputField.setAllowNull( rep.getStepAttributeBoolean( id_step, i, "parquet_nullable" ) );
        outputField.setDefaultValue( rep.getStepAttributeString( id_step, i, "parquet_default" ) );
        parquetOutputFields.add( outputField );
      }
      this.parquetOutputMeta.setOutputFields( parquetOutputFields );
    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    super.saveRep( rep, metaStore, id_transformation, id_step );
    try {
      for ( int i = 0; i < this.parquetOutputMeta.getOutputFields().size(); i++ ) {
        ParquetOutputField field = this.parquetOutputMeta.getOutputFields().get( i );
        rep.saveStepAttribute( id_transformation, id_step, i, "parquet_path", field.getFormatFieldName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "parquet_name", field.getPentahoFieldName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "parquet_type", field.getFormatType() );
        rep.saveStepAttribute( id_transformation, id_step, i, "parquet_precision", field.getPrecision() );
        rep.saveStepAttribute( id_transformation, id_step, i, "parquet_scale", field.getScale() );
        rep.saveStepAttribute( id_transformation, id_step, i, "parquet_nullable", field.getAllowNull() );
        rep.saveStepAttribute( id_transformation, id_step, i, "parquet_default", field.getDefaultValue() );
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to save step information to the repository for id_step=" + id_step, e );
    }
  }
}
