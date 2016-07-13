package io.druid.storage.ali;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.segment.loading.LoadSpec;
import io.druid.segment.loading.SegmentLoadingException;

import java.io.File;

/**
 * Created by libin on 15/12/18.
 */
@JsonTypeName(AliStorageDruidModule.SCHEME)
public class AliLoadSpec implements LoadSpec {
    @JsonProperty
    private final String path;

    private final AliDataSegmentPuller puller;

    @JsonCreator
    public AliLoadSpec(@JsonProperty("path") String path, @JacksonInject AliDataSegmentPuller puller) {

        this.path = path;
        this.puller = puller;
    }

    @Override
    public LoadSpecResult loadSegment(File outDir) throws SegmentLoadingException {
        return new LoadSpecResult(puller.getSegmentFiles(path, outDir).size());
    }
}
