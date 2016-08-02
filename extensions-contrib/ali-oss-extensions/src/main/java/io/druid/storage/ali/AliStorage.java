package io.druid.storage.ali;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObject;
import com.metamx.common.logger.Logger;
import io.druid.firehose.ali.StaticAliOssFirehoseFactory;

import java.io.InputStream;

/**
 * Created by libin on 15/12/18.
 */
public class AliStorage {
    final AliDataSegmentConfig config;
    final OSSClient client;
    private static final Logger log = new Logger(AliStorage.class);

    public AliStorage(AliDataSegmentConfig config) {
        this.config = config;
        this.client = new OSSClient(config.getEndpoint(), config.getKey(), config.getSecret());
    }

    public InputStream openStream(String file_name){

        OSSObject ossObject = this.client.getObject(this.config.getBucket(), file_name);
        log.info("read oss object from ali oss : file_name -- [%s], content length -- [%s]", file_name,  ossObject.getObjectMetadata().getContentLength());
        return ossObject.getObjectContent();
    }
}
