package io.druid.storage.ali;

import com.aliyun.oss.model.*;
import com.google.common.base.Predicates;
import com.google.inject.Inject;
import com.metamx.common.CompressionUtils;
import com.metamx.common.FileUtils;
import com.metamx.common.ISE;
import com.metamx.common.RetryUtils;
import com.metamx.common.logger.Logger;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;

import java.io.*;
import java.util.concurrent.Callable;

/**
 * Created by libin on 15/12/18.
 */
public class AliDataSegmentPuller extends AliStorage implements DataSegmentPuller{

    private static final Logger log = new Logger(AliDataSegmentPuller.class);

    @Inject
    public AliDataSegmentPuller(AliDataSegmentConfig config) {
        super(config);
    }

    @Override
    public void getSegmentFiles(DataSegment segment, File outDir) throws SegmentLoadingException {
        String path = (String) segment.getLoadSpec().get("path");
        getSegmentFiles(path,outDir);
    }

    public com.metamx.common.FileUtils.FileCopyResult getSegmentFiles(final String path, File outDir) throws SegmentLoadingException{
        if (!outDir.exists()) {
            outDir.mkdirs();
        }

        if (!outDir.isDirectory()) {
            throw new ISE("outDir[%s] must be a directory.", outDir);
        }

        long startTime = System.currentTimeMillis();
        final File tmpFile = new File(outDir, "index.zip");
        log.info("Pulling [%s] to temporary local cache  [%s]", path, tmpFile.getAbsolutePath());

        final com.metamx.common.FileUtils.FileCopyResult localResult;
        try {
            localResult = RetryUtils.retry(
                    new Callable<FileUtils.FileCopyResult>() {
                        @Override
                        public com.metamx.common.FileUtils.FileCopyResult call() throws Exception {
                            //TODO

                            try (OutputStream os = new FileOutputStream(tmpFile)) {
                                OSSObject object = client.getObject(new GetObjectRequest(config.getBucket(), path));
                                InputStream ins = object.getObjectContent();
                                int bytesRead = 0;
                                byte[] buffer = new byte[8192];
                                while ((bytesRead = ins.read(buffer, 0, 8192)) != -1) {
                                    os.write(buffer, 0, bytesRead);
                                }
                                ins.close();
                                os.close();
                            }

                            return new com.metamx.common.FileUtils.FileCopyResult(tmpFile);
                        }
                    },
                    Predicates.<Throwable>alwaysTrue(),
                    10
            );
        }catch (Exception e){
            throw new SegmentLoadingException(e, "Unable to copy key [%s] to file [%s]", path, tmpFile.getAbsolutePath());
        }


        try{
            final com.metamx.common.FileUtils.FileCopyResult result =  CompressionUtils.unzip(tmpFile, outDir);
            log.info(
                    "Pull of file[%s] completed in %,d millis (%s bytes)", path, System.currentTimeMillis() - startTime,
                    result.size()
            );
            return result;
        }
        catch (Exception e) {
            try {
                org.apache.commons.io.FileUtils.deleteDirectory(outDir);
            }
            catch (IOException e1) {
                log.error(e1, "Error clearing segment directory [%s]", outDir.getAbsolutePath());
                e.addSuppressed(e1);
            }
            throw new SegmentLoadingException(e, e.getMessage());
        } finally {
            if(!tmpFile.delete()){
                log.warn("Could not delete cache file at [%s]", tmpFile.getAbsolutePath());
            }
        }
    }
}
