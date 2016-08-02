package io.druid.firehose.ali;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.impl.FileIteratingFirehose;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.storage.ali.AliStorage;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

/**
 * Created by zhengyue on 16/8/1.
 */
public class StaticAliOssFirehoseFactory implements FirehoseFactory<StringInputRowParser> {

    private final AliStorage aliStorage;
    private final String file_name;
    private static final Logger log = new Logger(StaticAliOssFirehoseFactory.class);

    @JsonCreator
    public StaticAliOssFirehoseFactory(
            @JacksonInject("aliStorage") AliStorage aliStorage,
            @JsonProperty("file_name") String file_name
    ) {
        this.aliStorage = aliStorage;
        this.file_name = file_name;
    }

    @JsonProperty
    public String getFile_name() {
        return this.file_name;
    }

    @Override
    public Firehose connect(StringInputRowParser stringInputRowParser) throws IOException, ParseException {
        Preconditions.checkNotNull(aliStorage, "null aliStorage");

        Iterator<LineIterator> iterator = new Iterator<LineIterator>() {
            int i = 1;
            @Override
            public boolean hasNext() {
                return i > 0;
            }

            @Override
            public LineIterator next() {
                try{
                    i--;
                    InputStream inputStream = aliStorage.openStream(file_name);
                    return IOUtils.lineIterator(
                            new BufferedReader(
                                    new InputStreamReader(inputStream, Charsets.UTF_8)
                            )
                    );
                } catch (Exception e){
                    log.error(e,
                            "Exception opening file -- [%s]", file_name

                    );
                    throw Throwables.propagate(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        return new FileIteratingFirehose(iterator, stringInputRowParser);
    }
}

