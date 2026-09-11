#!/usr/bin/env python3

import argparse
import json
import logging
import sys
import unittest
from pathlib import Path


SUITE_DIR = Path("testing/trino-product-tests/src/test/java/io/trino/tests/product/suite")
SUITE_HELPERS = {"SuiteRunner", "SuiteTag"}

# This fork runs DuckLake's integration tests in plugin/trino-ducklake.
# Retain product coverage of shared SQL, clients, security, fault tolerance,
# compatibility, and Parquet. The excluded suites exercise other connectors
# or storage backends. Keep exclusions explicit so new suites require review.
EXCLUDED_SUITES = frozenset({
    "SuiteAllConnectorsSmoke",
    "SuiteAzure",
    "SuiteBlackHole",
    "SuiteCassandra",
    "SuiteClickhouse",
    "SuiteDeltaLakeAlluxioCaching",
    "SuiteDeltaLakeDatabricks133",
    "SuiteDeltaLakeDatabricks143",
    "SuiteDeltaLakeDatabricks154",
    "SuiteDeltaLakeDatabricks164",
    "SuiteDeltaLakeDatabricks173",
    "SuiteDeltaLakeDatabricks18",
    "SuiteDeltaLakeFloci",
    "SuiteDeltaLakeHdfs",
    "SuiteDeltaLakeOss",
    "SuiteExasol",
    "SuiteGcs",
    "SuiteHdfsImpersonation",
    "SuiteHive4",
    "SuiteHiveAlluxioCaching",
    "SuiteHiveBasic",
    "SuiteHiveSpark",
    "SuiteHiveStorageFormats",
    "SuiteHiveTransactional",
    "SuiteHmsOnly",
    "SuiteHudi",
    "SuiteIceberg",
    "SuiteIcebergVariants",
    "SuiteIgnite",
    "SuiteKafka",
    "SuiteLoki",
    "SuiteMysql",
    "SuitePostgresql",
    "SuiteRanger",
    "SuiteSnowflake",
    "SuiteSqlServer",
    "SuiteStorageFormatsDetailed",
    "SuiteTwoHives",
})

SUITES = [
    "SuiteFunctions",
    "SuiteTpch",
    "SuiteTpcds",
    "SuiteLdap",
    "SuiteOauth2",
    "SuiteClients",
    "SuiteJdbcKerberos",
    "SuiteTls",
    "SuiteSqlCancel",
    "SuiteAuthorization",
    "SuiteFaultTolerant",
    "SuiteParquet",
    "SuiteCompatibility",
]

ALL_SUITES = frozenset(SUITES)

# Include secondary connectors used by retained server-test environments.
# Empty mappings avoid scheduling suites for unrelated connector changes;
# unknown modules (including shared engine/libraries) still run all retained suites.
MODULE_TO_SUITES = {
    "plugin/trino-base-jdbc": {"SuiteClients"},
    "plugin/trino-blob-cache-alluxio": set(),
    "plugin/trino-exchange-filesystem": {"SuiteFaultTolerant"},
    "plugin/trino-example-jdbc": set(),
    "plugin/trino-ducklake": set(),
    "plugin/trino-spooling-filesystem": set(),
    "plugin/trino-bigquery": set(),
    "plugin/trino-blackhole": set(),
    "plugin/trino-cassandra": set(),
    "plugin/trino-clickhouse": set(),
    "plugin/trino-delta-lake": set(),
    "plugin/trino-druid": set(),
    "plugin/trino-duckdb": set(),
    "plugin/trino-elasticsearch": set(),
    "plugin/trino-exasol": set(),
    "plugin/trino-faker": set(),
    "plugin/trino-google-sheets": set(),
    "plugin/trino-hive": {
        "SuiteAuthorization",
        "SuiteClients",
        "SuiteCompatibility",
        "SuiteParquet",
        "SuiteSqlCancel",
        "SuiteTpcds",
        "SuiteTpch",
    },
    "plugin/trino-hoglake": set(),
    "plugin/trino-hudi": set(),
    "plugin/trino-iceberg": {"SuiteCompatibility"},
    "plugin/trino-ignite": set(),
    "plugin/trino-jmx": set(),
    "plugin/trino-kafka": set(),
    "plugin/trino-loki": set(),
    "plugin/trino-mariadb": set(),
    "plugin/trino-memory": {"SuiteClients", "SuiteJdbcKerberos"},
    "plugin/trino-mongodb": set(),
    "plugin/trino-mysql": set(),
    "plugin/trino-opensearch": set(),
    "plugin/trino-oracle": set(),
    "plugin/trino-pinot": set(),
    "plugin/trino-postgresql": {"SuiteClients"},
    "plugin/trino-prometheus": set(),
    "plugin/trino-redis": set(),
    "plugin/trino-redshift": set(),
    "plugin/trino-singlestore": set(),
    "plugin/trino-snowflake": set(),
    "plugin/trino-sqlserver": set(),
    "plugin/trino-tpcds": {"SuiteFunctions", "SuiteParquet", "SuiteTpcds"},
    "plugin/trino-thrift": set(),
    "plugin/trino-thrift-api": set(),
    "plugin/trino-thrift-testing-server": set(),
    "plugin/trino-ldap-group-provider": {"SuiteLdap"},
    "plugin/trino-password-authenticators": {"SuiteLdap"},
    "plugin/trino-ranger": set(),
    "plugin/trino-teradata-functions": {"SuiteFunctions"},
}


def main():
    parser = argparse.ArgumentParser(
        description="Build the JUnit product-test matrix, optionally filtered by GIB impacted modules."
    )
    parser.add_argument(
        "-i",
        "--impacted",
        type=argparse.FileType("r"),
        help="File containing affected Maven module paths, one per line",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        dest="loglevel",
        const=logging.INFO,
        default=logging.WARNING,
        help="Print matrix filtering decisions",
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        help="Test this script instead of executing it",
    )
    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel, format="%(levelname)s: %(message)s")

    if args.test:
        sys.argv = [sys.argv[0]]
        unittest.main()
        return

    impacted_modules = None
    if args.impacted is not None:
        impacted_modules = {line.strip() for line in args.impacted if line.strip()}
    print(json.dumps(build_matrix(impacted_modules)))


def build_matrix(impacted_modules):
    selected_suites = suites_for_impacted_modules(impacted_modules)
    if selected_suites is None:
        selected_suites = ALL_SUITES

    include = []
    for suite in SUITES:
        if suite in selected_suites:
            include.append({"suite": suite})
    return {"include": include} if include else {}


def suites_for_impacted_modules(impacted_modules):
    if impacted_modules is None:
        logging.info("Impact filtering was not requested; using the full product-test matrix")
        return None
    if not impacted_modules:
        logging.info("GIB reported no modules; using the full product-test matrix")
        return None

    unknown_modules = impacted_modules - MODULE_TO_SUITES.keys()
    if unknown_modules:
        logging.info(
            "Modules without an unambiguous product-test mapping (%s); using the full product-test matrix",
            ", ".join(sorted(unknown_modules)),
        )
        return None

    selected_suites = set()
    for module in impacted_modules:
        selected_suites.update(MODULE_TO_SUITES[module])
    logging.info("GIB impacted modules: %s", ", ".join(sorted(impacted_modules)))
    logging.info("Selected product-test suites: %s", ", ".join(sorted(selected_suites)))
    return selected_suites


def validate_configuration(suite_dir=SUITE_DIR):
    declared_suites = SUITES
    duplicate_suites = sorted({suite for suite in declared_suites if declared_suites.count(suite) > 1})
    if duplicate_suites:
        raise ValueError(f"Suites declared more than once: {', '.join(duplicate_suites)}")

    actual_suites = {path.stem for path in suite_dir.glob("Suite*.java")} - SUITE_HELPERS
    missing_suites = sorted(set(declared_suites) - actual_suites)
    if missing_suites:
        raise ValueError(f"Declared product test suites are missing: {', '.join(missing_suites)}")

    unwired_suites = sorted(actual_suites - set(declared_suites) - EXCLUDED_SUITES)
    if unwired_suites:
        raise ValueError(f"Product test suites are missing from the CI matrix: {', '.join(unwired_suites)}")

    invalid_mapped_suites = sorted(set().union(*MODULE_TO_SUITES.values()) - set(declared_suites))
    if invalid_mapped_suites:
        raise ValueError(f"Module mappings contain unknown suites: {', '.join(invalid_mapped_suites)}")

    missing_modules = sorted(module for module in MODULE_TO_SUITES if not Path(module, "pom.xml").is_file())
    if missing_modules:
        raise ValueError(f"Mapped Maven modules are missing: {', '.join(missing_modules)}")


class TestBuildMatrix(unittest.TestCase):
    def test_unrelated_connector_changes_produce_empty_matrix(self):
        for module in ("plugin/trino-mysql", "plugin/trino-kafka", "plugin/trino-delta-lake"):
            with self.subTest(module=module):
                self.assertEqual(build_matrix({module}), {})

    def test_ducklake_has_its_own_maven_tests(self):
        self.assertEqual(build_matrix({"plugin/trino-ducklake"}), {})

    def test_secondary_connector_keeps_client_coverage(self):
        self.assertEqual(
            suites_from_matrix(build_matrix({"plugin/trino-postgresql"})),
            {"SuiteClients"},
        )

    def test_hive_keeps_shared_server_and_parquet_coverage(self):
        self.assertEqual(
            suites_from_matrix(build_matrix({"plugin/trino-hive"})),
            {
                "SuiteAuthorization", "SuiteClients", "SuiteCompatibility", "SuiteParquet",
                "SuiteSqlCancel", "SuiteTpcds", "SuiteTpch",
            },
        )

    def test_excluded_suites_never_run_even_for_core_changes(self):
        for impacted in (None, set(), {"core/trino-main"}, {"plugin/trino-new-connector"}):
            with self.subTest(impacted=impacted):
                self.assertFalse(suites_from_matrix(build_matrix(impacted)) & EXCLUDED_SUITES)

    def test_core_change_runs_full_matrix(self):
        self.assertEqual(build_matrix({"core/trino-main"}), build_matrix(None))

    def test_shared_infrastructure_change_runs_full_matrix(self):
        self.assertEqual(build_matrix({"lib/trino-plugin-toolkit"}), build_matrix(None))

    def test_product_test_framework_change_runs_full_matrix(self):
        self.assertEqual(build_matrix({"testing/trino-product-tests"}), build_matrix(None))

    def test_unknown_module_runs_full_matrix(self):
        self.assertEqual(build_matrix({"plugin/trino-new-connector"}), build_matrix(None))

    def test_unknown_module_mixed_with_understood_module_runs_full_matrix(self):
        self.assertEqual(
            build_matrix({"plugin/trino-mysql", "plugin/trino-new-connector"}),
            build_matrix(None),
        )

    def test_empty_gib_output_runs_full_matrix(self):
        self.assertEqual(build_matrix(set()), build_matrix(None))

    def test_forced_full_run(self):
        matrix = build_matrix(None)
        self.assertEqual(suites_from_matrix(matrix), ALL_SUITES)
        self.assertEqual(len(matrix["include"]), len(ALL_SUITES))

    def test_understood_module_without_product_tests_produces_empty_matrix(self):
        self.assertEqual(build_matrix({"plugin/trino-example-jdbc"}), {})

    def test_matrix_is_complete(self):
        validate_configuration()


def suites_from_matrix(matrix):
    return {item["suite"] for item in matrix.get("include", [])}


validate_configuration()


if __name__ == "__main__":
    main()
