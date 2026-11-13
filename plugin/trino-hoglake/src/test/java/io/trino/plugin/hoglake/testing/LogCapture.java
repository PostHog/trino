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
package io.trino.plugin.hoglake.testing;

import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Everything logged under {@code io.trino} at any level while it is open:
 * the message, its parameters and the stack trace of what was thrown.
 */
public final class LogCapture
        implements Closeable
{
    private final List<String> logged = new CopyOnWriteArrayList<>();
    private final Logger logger = Logger.getLogger("io.trino");
    private final Level previousLevel = logger.getLevel();
    private final Handler handler = new Handler()
    {
        @Override
        public void publish(LogRecord record)
        {
            StringWriter text = new StringWriter();
            text.append(String.valueOf(record.getMessage()));
            if (record.getParameters() != null) {
                for (Object parameter : record.getParameters()) {
                    text.append(' ').append(String.valueOf(parameter));
                }
            }
            if (record.getThrown() != null) {
                record.getThrown().printStackTrace(new PrintWriter(text));
            }
            logged.add(text.toString());
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    };

    public LogCapture()
    {
        logger.setLevel(Level.ALL);
        logger.addHandler(handler);
    }

    public List<String> logged()
    {
        return logged;
    }

    @Override
    public void close()
    {
        logger.removeHandler(handler);
        logger.setLevel(previousLevel);
    }
}
