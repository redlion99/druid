package io.druid.storage.ali;

import com.aliyun.oss.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.common.CompressionUtils;
import com.metamx.common.logger.Logger;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

/**
 * Created by libin on 15/12/18.
 */
public class AliDataSegmentPusher extends AliStorage implements DataSegmentPusher {
    private final ObjectMapper jsonMapper;

    private static final Logger log = new Logger(AliDataSegmentPuller.class);

    @Inject
    public AliDataSegmentPusher(AliDataSegmentConfig config, ObjectMapper jsonMapper) {
        super(config);
        this.jsonMapper=jsonMapper;
    }

    @Override
    public String getPathForHadoop(String s) {
        return null;
    }

    @Override
    public DataSegment push(File indexFilesDir, DataSegment segment) throws IOException {
        final String storageDir = DataSegmentPusherUtil.getHdfsStorageDir(segment);
        String indexPath = String.format("%s/%s/index.zip", config.getStorageDirectory(), storageDir);
        String descriptorPath = String.format("%s/%s/descriptor.json", config.getStorageDirectory(), storageDir);
        String bucketName = config.getBucket();

        final File compressedIndexFile = File.createTempFile("druid", "index.zip");
        long indexSize = CompressionUtils.zip(indexFilesDir, compressedIndexFile);

        int version = SegmentUtils.getVersionFromDir(indexFilesDir);

        client.putObject(new PutObjectRequest(bucketName, indexPath, compressedIndexFile));

        log.info("Wrote compressed file [%s] to [%s]", compressedIndexFile.getAbsolutePath(), indexPath);

        segment = segment.withSize(indexSize)
                .withLoadSpec(
                        ImmutableMap.<String, Object> of("type", AliStorageDruidModule.SCHEME, "path", indexPath)
                )
                .withBinaryVersion(version);

        byte[] json = jsonMapper.writeValueAsBytes(segment);
        client.putObject(new PutObjectRequest(bucketName, descriptorPath, new ByteArrayInputStream(json),new ObjectMetadata()));


        log.info("Deleting zipped index File[%s]", compressedIndexFile);
        compressedIndexFile.delete();
        return segment;
    }
}
