package org.pentaho.big.data.kettle.plugins.formats.impl.waterline;

import org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output.ParquetOutput;
import org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output.ParquetOutputData;
import org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output.ParquetOutputMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public class WaterlineParquetOutput extends ParquetOutput {

  public WaterlineParquetOutput( StepMeta stepMeta,
                                 StepDataInterface stepDataInterface, int copyNr,
                                 TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    if ( !super.init( ((WaterlineCatalogWriterMeta) smi).getParquetOutputMeta(), sdi ) ) {
      return false;
    }
    meta = ((WaterlineCatalogWriterMeta) smi).getParquetOutputMeta();
    return true;
  }
}
