import os
import sys
from datetime import datetime, timedelta, timezone

from common import get_spark

spark = get_spark("maintenance")

SNAPSHOT_DAYS = int(os.environ.get("SNAPSHOT_RETENTION_DAYS", "3"))
ORPHAN_DAYS = max(1, int(os.environ.get("ORPHAN_RETENTION_DAYS", "3")))


def ts(days):
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


tables = []
for ns in ["bronze", "silver", "gold", "ops"]:
    try:
        for row in spark.sql(f"SHOW TABLES IN lh.{ns}").collect():
            tables.append(f"{ns}.{row['tableName']}")
    except Exception as e:
        print("RESULT skip namespace", ns, "-", str(e).splitlines()[0])

failures = 0

for tbl in tables:
    steps = [
        ("compact", f"""
            CALL lh.system.rewrite_data_files(
                table => '{tbl}',
                options => map(
                    'min-input-files', '5',
                    'max-file-group-size-bytes', '268435456',
                    'max-concurrent-file-group-rewrites', '1'))"""),
        ("manifests", f"CALL lh.system.rewrite_manifests(table => '{tbl}')"),
        ("expire", f"""
            CALL lh.system.expire_snapshots(
                table => '{tbl}',
                older_than => TIMESTAMP '{ts(SNAPSHOT_DAYS)}',
                retain_last => 5)"""),
        ("orphans", f"""
            CALL lh.system.remove_orphan_files(
                table => '{tbl}',
                older_than => TIMESTAMP '{ts(ORPHAN_DAYS)}')"""),
    ]
    for label, sql in steps:
        try:
            result = spark.sql(sql).collect()
            print("RESULT", tbl, label, "ok", [list(r) for r in result][:3])
        except Exception as e:
            failures += 1
            print("RESULT", tbl, label, "FAILED:", str(e).splitlines()[0])

if failures:
    print("RESULT maintenance finished with", failures, "failed step(s)")
    sys.exit(1)

print("RESULT maintenance finished for", len(tables), "tables")
