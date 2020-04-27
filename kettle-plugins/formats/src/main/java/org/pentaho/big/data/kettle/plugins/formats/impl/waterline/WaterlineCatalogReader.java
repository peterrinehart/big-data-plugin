/*!
 * HITACHI VANTARA PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2020 Hitachi Vantara. All rights reserved.
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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.serialization.BaseSerializingMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public class WaterlineCatalogReader extends BaseStep implements StepInterface {

  private WaterlineCatalogReaderMeta meta;

  public static final int DELIVERY_TAG_INDEX = 4;
  public WaterlineCatalogReader( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                       Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    boolean init = super.init( stepMetaInterface, stepDataInterface );

    BaseSerializingMeta serializingMeta = (BaseSerializingMeta) stepMetaInterface;
    meta = (WaterlineCatalogReaderMeta) serializingMeta.withVariables( this ); // handle variable substitution up-front

    if ( meta.getFilename().endsWith( ".parquet" ) ) {
      // initialize parquet reader
    } else {
      // intialize CSV reader
    }

    return init;
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    if ( meta.getFilename().endsWith( ".parquet" ) ) {
      // call parquet processRow method
    } else {
      // call Hadoop File Input processRow method
    }
    return true;
  }
}
