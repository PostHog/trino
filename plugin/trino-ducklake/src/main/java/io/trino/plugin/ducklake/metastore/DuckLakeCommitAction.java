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
package io.trino.plugin.ducklake.metastore;

/**
 * The catalog changes of one statement, expressed against the snapshot being created.
 * <p>
 * An action may be run more than once: a commit that loses a race against another writer is
 * discarded and replayed against the newer state. It must therefore derive everything it writes
 * from the commit it is given rather than from state captured beforehand.
 */
@FunctionalInterface
public interface DuckLakeCommitAction<T>
{
    T run(DuckLakeCommit commit);
}
