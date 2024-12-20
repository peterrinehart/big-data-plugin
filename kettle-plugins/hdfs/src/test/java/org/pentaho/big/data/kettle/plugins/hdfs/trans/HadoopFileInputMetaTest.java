/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.hdfs.trans;

import org.apache.commons.vfs2.provider.URLFileName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.fileinput.FileInputList;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.file.BaseFileField;
import org.pentaho.di.trans.steps.fileinput.text.TextFileFilter;
import org.pentaho.di.trans.steps.named.cluster.NamedClusterEmbedManager;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Vasilina Terehova
 */
public class HadoopFileInputMetaTest {

  public static final String TEST_CLUSTER_NAME = "TEST-CLUSTER-NAME";
  public static final String SAMPLE_HADOOP_FILE_INPUT_STEP = "sample-hadoop-file-input-step.xml";
  public static final String TEST_FILE_NAME = "test-file-name";
  public static final String TEST_FOLDER_NAME = "test-folder-name";
  private static Logger logger = LogManager.getLogger( HadoopFileInputMetaTest.class );
  // for message resolution
  private NamedClusterService namedClusterService;
  private HadoopFileSystemLocator hadoopFileSystemLocator;

  @Before
  public void setUp() throws Exception {
    namedClusterService = mock( NamedClusterService.class );
    hadoopFileSystemLocator = mock( HadoopFileSystemLocator.class );
  }

  /**
   * BACKLOG-7972 - Hadoop File Output: Hadoop Clusters dropdown doesn't preserve selected cluster after reopen a
   * transformation after changing signature of loadSource in , saveSource in HadoopFileOutputMeta wasn't called
   *
   * @throws Exception
   */
  @Test
  public void testSaveSourceCalledFromGetXml() throws Exception {
    HadoopFileInputMeta hadoopFileInputMeta = new HadoopFileInputMeta( namedClusterService, hadoopFileSystemLocator );
    hadoopFileInputMeta.allocateFiles( 1 );
    //create spy to check whether saveSource now is called
    HadoopFileInputMeta spy = initHadoopMetaInput( hadoopFileInputMeta );
    HashMap<String, String> mappings = new HashMap<>();
    mappings.put( TEST_FILE_NAME, HadoopFileOutputMetaTest.TEST_CLUSTER_NAME );
    spy.setNamedClusterURLMapping( mappings );
    StepMeta parentStepMeta = mock( StepMeta.class );
    TransMeta parentTransMeta = mock( TransMeta.class );
    when( parentStepMeta.getParentTransMeta() ).thenReturn( parentTransMeta );
    NamedClusterEmbedManager embedManager = mock( NamedClusterEmbedManager.class );
    when( parentTransMeta.getNamedClusterEmbedManager() ).thenReturn( embedManager );
    spy.setParentStepMeta( parentStepMeta );
    String xml = spy.getXML();
    Document hadoopOutputMetaStep = HadoopFileOutputMetaTest.getDocumentFromString( xml, new SAXBuilder() );
    Element fileElement = HadoopFileOutputMetaTest.getChildElementByTagName( hadoopOutputMetaStep.getRootElement(), "file" );
    //getting from file node cluster attribute value
    Element clusterNameElement =
      HadoopFileOutputMetaTest.getChildElementByTagName( fileElement, HadoopFileInputMeta.SOURCE_CONFIGURATION_NAME );
    assertEquals( TEST_CLUSTER_NAME, clusterNameElement.getValue() );
    //check that saveSource is called from TextFileOutputMeta
    verify( spy, times( 1 ) ).saveSource( any( StringBuilder.class ), any( String.class ) );
    verify( embedManager ).registerUrl( "test-file-name" );
  }

  private HadoopFileInputMeta initHadoopMetaInput( HadoopFileInputMeta hadoopFileInputMeta ) {
    HadoopFileInputMeta spy = Mockito.spy( hadoopFileInputMeta );
    when( spy.getFileName() ).thenReturn( new String[] {} );
    spy.setFileName( new String[] { TEST_FILE_NAME } );
    spy.setFilter( new TextFileFilter[] {} );
    spy.inputFields = new BaseFileField[] {};
    spy.inputFiles.fileMask = new String[] { TEST_FILE_NAME };
    spy.inputFiles.fileRequired = new String[] { TEST_FILE_NAME };
    spy.inputFiles.includeSubFolders = new String[] { TEST_FOLDER_NAME };
    spy.content.dateFormatLocale = Locale.getDefault();
    return spy;
  }

  public Node loadNodeFromXml( String fileName ) throws Exception {
    URL resource = getClass().getClassLoader().getResource( fileName );
    if ( resource == null ) {
      logger.error( "no file " + fileName + " found in resources" );
      throw new IllegalArgumentException( "no file " + fileName + " found in resources" );
    } else {
      return XMLHandler.getSubNode( XMLHandler.loadXMLFile( resource ), "entry" );
    }
  }

  @Test
  public void testLoadSourceCalledFromLoadXml() throws Exception {
    HadoopFileInputMeta hadoopFileInputMeta = new HadoopFileInputMeta( namedClusterService, hadoopFileSystemLocator );
    //set required data for step - empty
    HadoopFileInputMeta spy = Mockito.spy( hadoopFileInputMeta );
    Node node = loadNodeFromXml( SAMPLE_HADOOP_FILE_INPUT_STEP );
    //create spy to check whether saveSource now is called
    IMetaStore metaStore = mock( IMetaStore.class );
    spy.loadXML( node, Collections.emptyList(), metaStore );
    assertEquals( TEST_CLUSTER_NAME, hadoopFileInputMeta.getNamedClusterURLMapping().get( TEST_FILE_NAME ) );
    verify( spy, times( 1 ) ).loadSource( any( Node.class ), any( Node.class ), anyInt(), any( IMetaStore.class ) );
  }

  @Test
  public void testLoadSourceRepForUrlRefresh() throws Exception {
    final String URL_FROM_CLUSTER = "urlFromCluster";
    IMetaStore mockMetaStore = mock( IMetaStore.class );
    NamedCluster mockNamedCluster = mock( NamedCluster.class );
    when( mockNamedCluster.processURLsubstitution( any(), eq( mockMetaStore ), any() ) ).thenReturn( URL_FROM_CLUSTER );
    when( namedClusterService.getNamedClusterByName( TEST_CLUSTER_NAME, mockMetaStore ) ).thenReturn( mockNamedCluster );
    Repository mockRep = mock( Repository.class );
    when( mockRep.getJobEntryAttributeString( anyObject(), eq( 0 ), eq( "source_configuration_name" ) ) ).thenReturn( TEST_CLUSTER_NAME );
    HadoopFileInputMeta hadoopFileInputMeta =  new HadoopFileInputMeta( namedClusterService, hadoopFileSystemLocator );
    when( mockRep.getStepAttributeString( anyObject(), eq( 0 ), eq( "file_name" ) ) ).thenReturn( URL_FROM_CLUSTER );

    assertEquals( URL_FROM_CLUSTER, hadoopFileInputMeta.loadSourceRep( mockRep, null, 0, mockMetaStore ) );
  }

  @Test
  public void testGetFileInputList() {
    KettleLogStore.init();
    final String URL_FROM_CLUSTER = "urlFromCluster";
    StepMeta parentStepMeta = mock( StepMeta.class );
    IMetaStore mockMetaStore = mock( IMetaStore.class );
    NamedCluster mockNamedCluster = mock( NamedCluster.class );
    TransMeta parentTransMeta = mock( TransMeta.class );
    when( parentStepMeta.getParentTransMeta() ).thenReturn( parentTransMeta );
    when( parentTransMeta.getMetaStore() ).thenReturn( mockMetaStore );
    when( mockNamedCluster.processURLsubstitution( any(), eq( mockMetaStore ), any() ) ).thenReturn( URL_FROM_CLUSTER );
    when( namedClusterService.getNamedClusterByName( TEST_CLUSTER_NAME, mockMetaStore ) ).thenReturn( mockNamedCluster );
    HadoopFileInputMeta hadoopFileInputMetaSpy =  initHadoopMetaInput( new HadoopFileInputMeta( namedClusterService, hadoopFileSystemLocator ) );
    hadoopFileInputMetaSpy.environment = new String[] { TEST_CLUSTER_NAME };
    hadoopFileInputMetaSpy.setParentStepMeta( parentStepMeta );
    doReturn( new FileInputList() ).when( hadoopFileInputMetaSpy ).createFileList( any( VariableSpace.class ) );
    hadoopFileInputMetaSpy.getFileInputList( new Variables() );

    assertEquals( "urlFromCluster", hadoopFileInputMetaSpy.inputFiles.fileName[0] );
  }

  @Test
  public void testGetUrl() {
    final HadoopFileInputMeta meta = Mockito.mock( HadoopFileInputMeta.class );
    final URLFileName mockFileName = Mockito.mock( URLFileName.class );
    final String scheme = "hdfs";
    final String hostName = "svqxbdcn6cdh512n1.pentahoqa.com";
    final String rootUrl = scheme + "://" + hostName + ":8020/";
    final String path = "wordcount/input";
    final String url = rootUrl + path;

    Mockito.doReturn( hostName ).when( mockFileName ).getHostName();
    Mockito.doReturn( scheme ).when( mockFileName ).getScheme();

    Mockito.doReturn( mockFileName ).when( meta ).getUrlFileName( url );
    Mockito.doReturn( rootUrl ).when( mockFileName ).getRootURI();
    Mockito.doCallRealMethod().when( meta ).getUrlHostName( url );
    Mockito.doCallRealMethod().when( meta ).getUrlPath( url );

    Assert.assertEquals( hostName, meta.getUrlHostName( url ) );
    Assert.assertEquals( "/" + path, meta.getUrlPath( url ) );
  }

  @Test
  public void testEncryption() throws Exception {
    KettleEnvironment.init();
    HadoopFileInputMeta meta = new HadoopFileInputMeta();
    String url = "hdfs://user:password@myhost:8020/myfile";
    String encrypted = meta.encryptDecryptPassword( url, HadoopFileInputMeta.EncryptDirection.ENCRYPT );
    assertTrue( !encrypted.contains( "password" ) );
    assertEquals( url, meta.encryptDecryptPassword( encrypted, HadoopFileInputMeta.EncryptDirection.DECRYPT ) );
  }

  @Test
  public void testNoPassword() throws Exception {
    KettleEnvironment.init();
    HadoopFileInputMeta meta = new HadoopFileInputMeta();
    String url = "hdfs://user@myhost:8020/myfile";
    String encrypted = meta.encryptDecryptPassword( url, HadoopFileInputMeta.EncryptDirection.ENCRYPT );
    assertEquals( url, meta.encryptDecryptPassword( encrypted, HadoopFileInputMeta.EncryptDirection.DECRYPT ) );
  }

  @Test
  public void testCanAccessHdfs() throws ClusterInitializationException {
    StepMeta parentStepMeta = mock( StepMeta.class );
    IMetaStore mockMetaStore = mock( IMetaStore.class );
    TransMeta parentTransMeta = mock( TransMeta.class );
    when( parentStepMeta.getParentTransMeta() ).thenReturn( parentTransMeta );
    when( parentTransMeta.getMetaStore() ).thenReturn( mockMetaStore );

    HadoopFileInputMeta hadoopFileInputMeta = initHadoopMetaInput( new HadoopFileInputMeta( namedClusterService, hadoopFileSystemLocator ) );
    hadoopFileInputMeta.setParentStepMeta( parentStepMeta );
    NamedCluster mockCluster = mock( NamedCluster.class );
    when( namedClusterService.getNamedClusterByName( eq( "fooCluster" ), nullable( IMetaStore.class ) ) ).thenReturn( mockCluster );
    when( namedClusterService.getNamedClusterByHost( eq( "fooCluster" ), nullable( IMetaStore.class ) ) ).thenReturn( mockCluster );
    when( hadoopFileSystemLocator.getHadoopFilesystem( eq( mockCluster ), nullable( URI.class ) ) ).thenReturn( null );
    doCallRealMethod().when( hadoopFileInputMeta ).canAccessHdfs( anyString(), eq( true ) );

    assertFalse( "Error checking hdfs file system", hadoopFileInputMeta.canAccessHdfs( "hdfs://fooCluster:8080/my/file/path", true ) );
    assertFalse( "Error checking hc file system", hadoopFileInputMeta.canAccessHdfs( "hc://fooCluster/my/file/path", true ) );
    assertTrue( "Error checking hc file system", hadoopFileInputMeta.canAccessHdfs( "file://some/file/path", true ) );
    assertTrue( "Error checking hc file system", hadoopFileInputMeta.canAccessHdfs( "hc://fooCluster/my/file/path", false ) );
  }
}
