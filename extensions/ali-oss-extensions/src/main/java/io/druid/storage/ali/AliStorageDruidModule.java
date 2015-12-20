package io.druid.storage.ali;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import io.druid.guice.Binders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.PolyBind;
import io.druid.initialization.DruidModule;
import io.druid.segment.loading.DataSegmentPusher;

import java.util.List;

/**
 * Created by libin on 15/12/18.
 */
public class AliStorageDruidModule  implements DruidModule{
    public static final String SCHEME = "ali";
    @Override
    public List<? extends Module> getJacksonModules() {
        return ImmutableList.of(
                new com.fasterxml.jackson.databind.Module() {
                    @Override
                    public String getModuleName() {
                        return "DruidAliOssStorage-" + System.identityHashCode(this);
                    }

                    @Override
                    public Version version() {
                        return Version.unknownVersion();
                    }

                    @Override
                    public void setupModule(SetupContext context) {
                        context.registerSubtypes(AliLoadSpec.class);
                    }
                }
        );
    }

    @Override
    public void configure(Binder binder) {
        Binders.dataSegmentPullerBinder(binder)
                .addBinding(SCHEME)
                .to(AliDataSegmentPuller.class)
                .in(LazySingleton.class);

        PolyBind.optionBinder(binder, Key.get(DataSegmentPusher.class))
                .addBinding(SCHEME)
                .to(AliDataSegmentPusher.class)
                .in(LazySingleton.class);
        JsonConfigProvider.bind(binder, "druid.storage", AliDataSegmentConfig.class);
    }
}
