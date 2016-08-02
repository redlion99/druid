package io.druid.storage.ali;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObject;

import java.io.InputStream;

/**
 * Created by libin on 15/12/18.
 */
public class AliStorage {
    final AliDataSegmentConfig config;
    final OSSClient client;

    public AliStorage(AliDataSegmentConfig config) {
        this.config = config;
        this.client = new OSSClient(config.getEndpoint(), config.getKey(), config.getSecret());
    }

    public InputStream openStream(String file_name){
        OSSObject ossObject = this.client.getObject(this.config.getBucket(), file_name);
        return ossObject.getObjectContent();
    }
}
