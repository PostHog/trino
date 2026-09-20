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
package io.trino.connector;

/**
 * Why a coordinator is not following the published catalogs, or why a security component cannot say
 * what it has loaded. This is everything the readiness endpoint reports about failures, so it is a
 * closed set of stable categories and never the text of the underlying failure: a store error can
 * carry a JDBC URL, a property value or a credential, an authorization backend error can carry its
 * endpoint, and a caller authorized to read readiness is not thereby authorized to read those. The
 * details stay in the server log.
 */
public enum CatalogSyncFailure
{
    /**
     * The catalog store cannot be reached or read at all.
     */
    STORE_UNREACHABLE,

    /**
     * Nothing has ever been published for this cell. Not an empty desired state: a store that was
     * restored, emptied or never adopted is indistinguishable from one that was never written.
     */
    NOTHING_PUBLISHED,

    /**
     * The published revision is lower than the one already applied here. The state went backwards,
     * so this coordinator freezes on what it has instead of removing working catalogs.
     */
    REVISION_REGRESSED,

    /**
     * The published state could not be read completely - a row count that disagrees with the
     * published one, or a row that cannot be parsed. Never applied as a smaller set of catalogs.
     */
    SNAPSHOT_INCOMPLETE,

    /**
     * Catalogs of the published revision could not be installed locally, so the revision is not
     * acknowledged. The catalogs that were working here are untouched.
     */
    CATALOGS_NOT_APPLIED,

    /**
     * The initial catalogs are still loading; nothing has been reconciled yet.
     */
    NOT_INITIALIZED,

    /**
     * The configured catalog store does not publish revisions, so there is nothing to follow.
     */
    STORE_NOT_REVISIONED,

    /**
     * The catalog store is managed by an external publisher, but this coordinator was not
     * configured to follow it. It serves the catalogs it loaded at startup and nothing else.
     */
    SYNCHRONIZATION_DISABLED,

    /**
     * The configured component cannot report the configuration data it has loaded at all. Not an
     * acknowledgement, and not something a controller can wait for.
     */
    COMPONENT_DOES_NOT_REPORT,

    /**
     * The component can report what it has loaded, but is missing the configuration it needs to do
     * so - an OPA access control without a revision URI, for instance.
     */
    COMPONENT_NOT_CONFIGURED,

    /**
     * The component tried and failed: its data could not be read, or the backend that holds it did
     * not answer. Nothing is acknowledged.
     */
    COMPONENT_UNAVAILABLE,
}
