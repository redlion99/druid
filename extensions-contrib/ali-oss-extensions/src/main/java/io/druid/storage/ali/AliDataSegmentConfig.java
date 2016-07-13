package io.druid.storage.ali;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

/**
 * Created by libin on 15/12/18.
 */
public class AliDataSegmentConfig  {

    @JsonProperty
    @NotNull
    private  String endpoint;


    @JsonProperty
    @NotNull
    private  String bucket;

    @JsonProperty
    @NotNull
    private  String key;

    @JsonProperty
    @NotNull
    private  String secret;

    @JsonProperty
    @NotNull
    private  String storageDirectory ="/";

    public String getEndpoint() {
        return endpoint;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public String getSecret() {
        return secret;
    }

    public String getStorageDirectory() {
        return storageDirectory;
    }
}
