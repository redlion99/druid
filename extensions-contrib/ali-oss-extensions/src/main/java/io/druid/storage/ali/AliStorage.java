package io.druid.storage.ali;

import com.aliyun.oss.OSSClient;

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
}
