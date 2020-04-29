/*!
 * HITACHI VANTARA PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2019 Hitachi Vantara. All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Hitachi Vantara and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Hitachi Vantara and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Hitachi Vantara is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Hitachi Vantara,
 * explicitly covering such access.
 */
package org.pentaho.big.data.kettle.plugins.formats.impl.waterline;

import org.apache.commons.lang.Validate;
import org.apache.commons.vfs2.FileName;
import org.pentaho.big.data.kettle.plugins.formats.FormatInputFile;
import org.pentaho.big.data.kettle.plugins.formats.impl.NamedClusterResolver;
import org.pentaho.big.data.kettle.plugins.formats.impl.parquet.input.ParquetInput;
import org.pentaho.big.data.kettle.plugins.formats.impl.parquet.input.ParquetInputData;
import org.pentaho.big.data.kettle.plugins.formats.impl.parquet.input.ParquetInputMeta;
import org.pentaho.big.data.kettle.plugins.formats.parquet.ParquetTypeConverter;
import org.pentaho.big.data.kettle.plugins.formats.parquet.input.ParquetInputField;
import org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileMeta;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.fileinput.FileInputList;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.GenericStepData;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.fileinput.text.TextFileInput;
import org.pentaho.di.trans.steps.fileinput.text.TextFileInputMeta;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Step( id = "WaterlineCatalogReader", image = "WaterlineCatalogReader.svg", name = "Waterline Catalog Reader",
  description = "Read file from the catalog", categoryDescription = "Big Data" )
@InjectionSupported( localizationPrefix = "WaterlineCatalogReaderMeta.Injection." )
public class WaterlineCatalogReaderMeta extends TextFileInputMeta implements HadoopFileMeta {

  public static final String FILE_NAME = "FILE_NAME";

  private Map<String, String> namedClusterURLMapping = null;

  public static final String SOURCE_CONFIGURATION_NAME = "source_configuration_name";
  public static final String LOCAL_SOURCE_FILE = "LOCAL-SOURCE-FILE-";
  public static final String STATIC_SOURCE_FILE = "STATIC-SOURCE-FILE-";
  public static final String S3_SOURCE_FILE = "S3-SOURCE-FILE-";
  public static final String S3_DEST_FILE = "S3-DEST-FILE-";

  public ParquetInputField[] parquetInputFields;

  /**
   * The environment of the selected file/folder
   */
  @Injection( name = "ENVIRONMENT", group = "FILENAME_LINES" )
  public String[] environment = {};

  public NamedClusterService getNamedClusterService() {
    return namedClusterService;
  }

  public void setNamedClusterService( NamedClusterService namedClusterService ) {
    this.namedClusterService = namedClusterService;
  }

  enum EncryptDirection { ENCRYPT, DECRYPT }

  //@Injection( name = FILE_NAME ) private String fileName;

 // private List< ? extends IParquetInputField> parquetInputFields;

  private NamedClusterResolver namedClusterResolver;
  private NamedClusterService namedClusterService;

  public WaterlineCatalogReaderMeta() {
    this( null, null );
  }

  public WaterlineCatalogReaderMeta( NamedClusterResolver namedClusterResolver, NamedClusterService namedClusterService ) {
    super();
    namedClusterURLMapping = new HashMap<>();
    this.setNamedClusterResolver( namedClusterResolver );
    this.setNamedClusterService( namedClusterService );
  }

//  @Override public void setDefault() {
//    return;
//  }

  // TODO:
  //  1) save/retrieve parquet fields from xml
  //  2) add and test logic to run different step for pqt
  //  3) clean up

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                                Trans trans ) {
    if ( getFilename().endsWith( ".pqt" ) ) {
      // initialize parquet reader
      return new WaterlineParquetInput( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    } else {
      // intialize CSV reader
      return new TextFileInput( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    }
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
                     String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository,
                     IMetaStore metaStore ) {
    super.check( remarks, transMeta, stepMeta, prev, input, output, info, space, repository, metaStore );
  }

  @Override public StepDataInterface getStepData() {
    if ( getFilename().endsWith( ".pqt" ) ) {
      return new ParquetInputData();
    } else {
      return super.getStepData();
    }
  }

  @Override public String getDialogClassName() {
    return "org.pentaho.big.data.kettle.plugins.formats.impl.waterline.WaterlineCatalogReaderDialog";
  }

  public String getFilename() {
    if ( inputFiles != null && inputFiles.fileName != null
      && inputFiles.fileName.length > 0 ) {
      return inputFiles.fileName[ 0 ];
    } else {
      return null;
    }
  }

  public void setFilename( String filename ) {
    inputFiles.fileName[ 0 ] = filename;
  }

  @Override public String getEncoding() {
    return null;
  }

  protected ParquetInputMeta getParquetInputMeta() {
    ParquetInputMeta parquetInputMeta = new ParquetInputMeta( getNamedClusterResolver() );
    parquetInputMeta.allocateFiles( 1 );
    parquetInputMeta.setFilename( getFilename() );
    //populateParquetInputFields( parquetInputMeta );
    parquetInputMeta.setInputFields( parquetInputFields );
    return parquetInputMeta;
  }

//  private List<? extends IParquetInputField> getInputFieldsFromParquetFile( boolean failQuietly ) {
//    String parquetFileName = getFilename();
//    List<? extends IParquetInputField> inputFields = null;
//    try {
//      inputFields = ParquetInput.retrieveSchema( this.getNamedClusterResolver().getNamedClusterServiceLocator(),
//        this.getNamedClusterResolver().resolveNamedCluster( parquetFileName ), parquetFileName );
//    } catch ( Exception ex ) {
//      if ( !failQuietly ) {
//        logError( "Error getting fields from Parquet file", ex );
//      }
//    }
//    return inputFields;
//  }

//  private void populateParquetInputFields( ParquetInputMeta meta ) {
//    List<? extends IParquetInputField> actualParquetFileInputFields = getInputFieldsFromParquetFile( false );
//
//    int nrFields = actualParquetFileInputFields.size();
//    meta.setInputFields( new ParquetInputField[ nrFields ] );
//    int i = 0;
//    for ( IParquetInputField fileField : actualParquetFileInputFields ) {
//      ParquetInputField field = new ParquetInputField();
//      field.setFormatFieldName( fileField.getFormatFieldName() );
//      field.setFormatType( fileField.getFormatType() );
//      field.setPentahoFieldName( fileField.getFormatFieldName() );
//      field.setPentahoType( fileField.getPentahoType() );
//      field.setStringFormat( fileField.getStringFormat() );
//      meta.inputFields[ i++ ] = field;
//    }
//  }

  public NamedClusterResolver getNamedClusterResolver() {
    return namedClusterResolver;
  }

  public void setNamedClusterResolver(
    NamedClusterResolver namedClusterResolver ) {
    this.namedClusterResolver = namedClusterResolver;
  }

  @Override
  protected String loadSource( Node filenode, Node filenamenode, int i, IMetaStore metaStore ) {
    String source_filefolder = XMLHandler.getNodeValue( filenamenode );
    Node sourceNode = XMLHandler.getSubNodeByNr( filenode, SOURCE_CONFIGURATION_NAME, i );
    String source = XMLHandler.getNodeValue( sourceNode );
    try {
      return source_filefolder == null ? null
        : loadUrl( encryptDecryptPassword( source_filefolder, EncryptDirection.DECRYPT ), source, metaStore,
        namedClusterURLMapping );
    } catch ( Exception ex ) {
      // Do nothing
    }
    return null;
  }

  @Override
  protected void saveSource( StringBuilder retVal, String source ) {
    String namedCluster = namedClusterURLMapping.get( source );
    retVal.append( "      " )
      .append( XMLHandler.addTagValue( "name", encryptDecryptPassword( source, EncryptDirection.ENCRYPT ) ) );
    retVal.append( "          " ).append( XMLHandler.addTagValue( SOURCE_CONFIGURATION_NAME, namedCluster ) );
  }

  // Receiving metaStore because RepositoryProxy.getMetaStore() returns a hard-coded null
  @Override
  protected String loadSourceRep( Repository rep, ObjectId id_step, int i, IMetaStore metaStore )
    throws KettleException {
    String source_filefolder = rep.getStepAttributeString( id_step, i, "file_name" );
    String ncName = rep.getJobEntryAttributeString( id_step, i, SOURCE_CONFIGURATION_NAME );
    return loadUrl( encryptDecryptPassword( source_filefolder, EncryptDirection.DECRYPT ), ncName, metaStore,
      namedClusterURLMapping );
  }

  @Override
  protected void saveSourceRep( Repository rep, ObjectId id_transformation, ObjectId id_step, int i, String fileName )
    throws KettleException {
    String namedCluster = namedClusterURLMapping.get( fileName );
    rep.saveStepAttribute( id_transformation, id_step, i, "file_name",
      encryptDecryptPassword( fileName, EncryptDirection.ENCRYPT ) );
    rep.saveStepAttribute( id_transformation, id_step, i, SOURCE_CONFIGURATION_NAME, namedCluster );
  }

  public String loadUrl( String url, String ncName, IMetaStore metastore, Map<String, String> mappings ) {
    NamedCluster c = namedClusterService.getNamedClusterByName( ncName, metastore );
    if ( c != null ) {
      url = c.processURLsubstitution( url, metastore, new Variables() );
    }
    if ( !Utils.isEmpty( ncName ) && !Utils.isEmpty( url ) && mappings != null ) {
      mappings.put( url, ncName );
      // in addition to the url as-is, add the public uri string version of the url (hidden password) to the map,
      // since that is the value that the data-lineage analyzer will have access to for cluster lookup
      try {
        mappings.put( getFriendlyUri( url ).toString(), ncName );
      } catch ( final Exception e ) {
        // no-op
      }
    }
    return url;
  }

  public void setNamedClusterURLMapping( Map<String, String> mappings ) {
    this.namedClusterURLMapping = mappings;
  }

  public Map<String, String> getNamedClusterURLMapping() {
    return this.namedClusterURLMapping;
  }

  @Override
  public String getClusterName( final String url ) {
    String clusterName = null;
    try {
      URI friendlyUri = getFriendlyUri( url );
      clusterName = getClusterNameBy( friendlyUri.toString() );
    } catch ( final URISyntaxException e ) {
      // no-op
    }
    return clusterName;
  }

  private URI getFriendlyUri( String url ) throws URISyntaxException {
    URI origUri = new URI( url );
    return new URI( origUri.getScheme(), null, origUri.getHost(), origUri.getPort(),
      origUri.getPath(), origUri.getQuery(), origUri.getFragment() );
  }

  public String getClusterNameBy( String url ) {
    return this.namedClusterURLMapping.get( url );
  }

  public String getUrlPath( String incomingURL ) {
    String path = null;
    FileName fileName = getUrlFileName( incomingURL );
    if ( fileName != null ) {
      String root = fileName.getRootURI();
      path = incomingURL.substring( root.length() - 1 );
    }
    return path;
  }

  @Override
  public FileInputList getFileInputList( VariableSpace space ) {
    inputFiles.normalizeAllocation( inputFiles.fileName.length );
    for ( int i = 0; i < environment.length; i++ ) {
      if ( inputFiles.fileName[ i ].contains( "://" ) ) {
        continue;
      }
      String sourceNc = environment[ i ];
      sourceNc = sourceNc.equals( WaterlineCatalogReaderDialog.LOCAL_ENVIRONMENT ) ? WaterlineCatalogReaderMeta.LOCAL_SOURCE_FILE + i : sourceNc;
      sourceNc = sourceNc.equals( WaterlineCatalogReaderDialog.STATIC_ENVIRONMENT ) ? WaterlineCatalogReaderMeta.STATIC_SOURCE_FILE + i : sourceNc;
      sourceNc = sourceNc.equals( WaterlineCatalogReaderDialog.S3_ENVIRONMENT ) ? WaterlineCatalogReaderMeta.S3_SOURCE_FILE + i : sourceNc;
      String source = inputFiles.fileName[ i ];
      if ( !Utils.isEmpty( source ) ) {
        inputFiles.fileName[ i ] =
          loadUrl( source, sourceNc, getParentStepMeta().getParentTransMeta().getMetaStore(), null );
      } else {
        inputFiles.fileName[ i ] = "";
      }
    }
    return createFileList( space );
  }

  /**
   * Created for test purposes
   */
  FileInputList createFileList( VariableSpace space ) {
    return FileInputList.createFileList( space, inputFiles.fileName, inputFiles.fileMask, inputFiles.excludeFileMask,
      inputFiles.fileRequired, inputFiles.includeSubFolderBoolean() );
  }

  protected String encryptDecryptPassword( String source, EncryptDirection direction ) {
    Validate.notNull( direction, "'direction' must not be null" );
    try {
      URI uri = new URI( source );
      String userInfo = uri.getUserInfo();
      if ( userInfo != null ) {
        String[] userInfoArray = userInfo.split( ":", 2 );
        if ( userInfoArray.length < 2 ) {
          return source; //no password present
        }
        String password = userInfoArray[ 1 ];
        String processedPassword;
        switch ( direction ) {
          case ENCRYPT:
            processedPassword = Encr.encryptPasswordIfNotUsingVariables( password );
            break;
          case DECRYPT:
            processedPassword = Encr.decryptPasswordOptionallyEncrypted( password );
            break;
          default:
            throw new InvalidParameterException( "direction must be 'ENCODE' or 'DECODE'" );
        }
        URI encryptedUri =
          new URI( uri.getScheme(), userInfoArray[ 0 ] + ":" + processedPassword, uri.getHost(), uri.getPort(),
            uri.getPath(), uri.getQuery(), uri.getFragment() );
        return encryptedUri.toString();
      }
    } catch ( URISyntaxException e ) {
      return source; // if this is non-parseable as a uri just return the source without changing it.
    }
    return source; // Just for the compiler should NEVER hit this code
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 1500 );
    retval.append( super.getXML() );

    retval.append( "    <parquet_fields>" ).append( Const.CR );
    for ( int i = 0; i < parquetInputFields.length; i++ ) {
      ParquetInputField field = parquetInputFields[ i ];
      retval.append( "      <field>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "parquet_path", field.getFormatFieldName() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "parquet_field_name", field.getPentahoFieldName() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "parquet_field_type", field.getTypeDesc() ) );
      ParquetSpec.DataType parquetType = field.getParquetType();
      if ( parquetType != null  && !parquetType.equals( ParquetSpec.DataType.NULL ) ) {
        retval.append( "        " )
          .append( XMLHandler.addTagValue( "parquet_type", parquetType.getName() ) );
      } else {
        retval.append( "        " )
          .append( XMLHandler.addTagValue( "parquet_type", ParquetTypeConverter.convertToParquetType( field.getTypeDesc() ) ) );
      }
      if ( field.getStringFormat() != null ) {
        retval.append( "        " ).append( XMLHandler.addTagValue( "parquet_format", field.getStringFormat() ) );
      }
      retval.append( "      </field>" ).append( Const.CR );
    }
    retval.append( "    </parquet_fields>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      super.saveRep( rep, metaStore, id_transformation, id_step );

      for ( int i = 0; i < parquetInputFields.length; i++ ) {
        ParquetInputField field = parquetInputFields[ i ];

        rep.saveStepAttribute( id_transformation, id_step, i, "parquet_path", field.getFormatFieldName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "parquet_field_name", field.getPentahoFieldName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "parquet_field_type", field.getTypeDesc() );
        ParquetSpec.DataType parquetType = field.getParquetType();
        if ( parquetType != null  && !parquetType.equals( ParquetSpec.DataType.NULL ) ) {
          rep.saveStepAttribute( id_transformation, id_step, i, "parquet_type", parquetType.getName() );
        } else {
          rep.saveStepAttribute( id_transformation, id_step, i, "parquet_type", ParquetTypeConverter.convertToParquetType( field.getTypeDesc() ) );
        }
        if ( field.getStringFormat() != null ) {
          rep.saveStepAttribute( id_transformation, id_step, i, "parquet_format", field.getStringFormat() );
        }
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to save step information to the repository for id_step=" + id_step, e );
    }
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    super.loadXML( stepnode, databases, metaStore );
    Node parquetFieldsNode = XMLHandler.getSubNode( stepnode, "parquet_fields" );
    int nrParquetfields = XMLHandler.countNodes( parquetFieldsNode, "field" );

    this.parquetInputFields = new ParquetInputField[ nrParquetfields ];
    for ( int i = 0; i < nrParquetfields; i++ ) {
      Node fnode = XMLHandler.getSubNodeByNr( parquetFieldsNode, "field", i );

      ParquetInputField field = new ParquetInputField();
      field.setFormatFieldName( XMLHandler.getTagValue( fnode, "parquet_path" ) );
      field.setPentahoFieldName( XMLHandler.getTagValue( fnode, "parquet_field_name" ) );
      field.setPentahoType( ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( fnode, "parquet_field_type" ) ) );
      String parquetType = XMLHandler.getTagValue( fnode, "parquet_type" );
      if ( parquetType != null && !parquetType.equalsIgnoreCase( "null" ) ) {
        field.setParquetType( parquetType );
      } else {
        field.setParquetType( ParquetTypeConverter.convertToParquetType( field.getPentahoType() ) );
      }

      String stringFormat = XMLHandler.getTagValue( fnode, "parquet_format" );
      field.setStringFormat( stringFormat == null ? "" : stringFormat );
      this.parquetInputFields[ i ] = field;
    }
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {
      super.readRep( rep, metaStore, id_step, databases );

      int nrfields = rep.countNrStepAttributes( id_step, "parquet_field_name" );
      this.parquetInputFields = new ParquetInputField[ nrfields ];
      for ( int i = 0; i < nrfields; i++ ) {
        ParquetInputField field = new ParquetInputField();
        field.setFormatFieldName( rep.getStepAttributeString( id_step, i, "parquet_path" ) );
        field.setPentahoFieldName( rep.getStepAttributeString( id_step, i, "parquet_field_name" ) );
        field.setPentahoType( rep.getStepAttributeString( id_step, i, "parquet_field_type" ) );
        String parquetType = rep.getStepAttributeString( id_step, i, "parquet_type" );
        if ( parquetType != null && !parquetType.equalsIgnoreCase( "null" ) ) {
          field.setParquetType( parquetType );
        } else {
          field.setParquetType( ParquetTypeConverter.convertToParquetType( field.getPentahoType() ) );
        }
        String stringFormat = rep.getStepAttributeString( id_step, i, "parquet_format" );
        field.setStringFormat( stringFormat == null ? "" : stringFormat );
        this.parquetInputFields[ i ] = field;
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

}
