/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hoglake;

import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;

public class HoglakeConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "hoglake";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);
        HoglakeConfig hoglakeConfig = HoglakeConfig.fromMap(config);

        HoglakeClient client = new HoglakeClient(
                hoglakeConfig.uri(), hoglakeConfig.catalog(), hoglakeConfig.requestTimeout());

        S3FileSystemConfig s3Config = new S3FileSystemConfig()
                .setRegion(hoglakeConfig.s3Region())
                .setEndpoint(hoglakeConfig.s3Endpoint())
                .setAwsAccessKey(hoglakeConfig.s3AccessKey())
                .setAwsSecretKey(hoglakeConfig.s3SecretKey())
                .setPathStyleAccess(hoglakeConfig.s3PathStyle());
        S3FileSystemFactory fileSystemFactory =
                new S3FileSystemFactory(context.getOpenTelemetry(), s3Config, new S3FileSystemStats());

        return new HoglakeConnector(
                new HoglakeMetadata(client),
                new HoglakeSplitManager(client),
                new HoglakePageSourceProvider(fileSystemFactory),
                fileSystemFactory,
                client);
    }
}
