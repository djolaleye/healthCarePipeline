"""
Guards the silent integration seams between src/core/schema.py and template.yaml.
"""
from pathlib import Path

import pytest
import yaml

from core import schema

TEMPLATE = Path(__file__).resolve().parent.parent / "template.yaml"


class _CfnLoader(yaml.SafeLoader):
    """SafeLoader that tolerates CloudFormation short-form tags (!Ref, !Sub, ...)."""


def _any_tag(loader, _suffix, node):
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node, deep=True)
    return loader.construct_mapping(node, deep=True)


_CfnLoader.add_multi_constructor("!", _any_tag)


def _as_list(value):
    return value if isinstance(value, list) else [value]


def _by_type(resources, cfn_type):
    return {lid: r for lid, r in resources.items() if r["Type"] == cfn_type}


@pytest.fixture(scope="module")
def resources():
    return yaml.load(TEMPLATE.read_text(), Loader=_CfnLoader)["Resources"]


@pytest.fixture(scope="module")
def transform(resources):
    functions = _by_type(resources, "AWS::Serverless::Function")
    assert len(functions) == 1, "expected exactly one function (the transform)"
    (logical_id, resource), = functions.items()
    return logical_id, resource["Properties"]


@pytest.fixture(scope="module")
def analytics_bucket(transform):
    """Logical ID of the analytics bucket, via the function's !Ref env var."""
    _, props = transform
    return props["Environment"]["Variables"]["ANALYTICS_BUCKET"]


@pytest.fixture(scope="module")
def raw_stream(resources):
    streams = _by_type(resources, "AWS::KinesisFirehose::DeliveryStream")
    assert len(streams) == 1, "expected exactly one Firehose (the raw backup)"
    (_, resource), = streams.items()
    return resource["Properties"]


def _glue_table(resources, name):
    tables = [
        r for r in _by_type(resources, "AWS::Glue::Table").values()
        if r["Properties"]["TableInput"]["Name"] == name
    ]
    assert len(tables) == 1, name
    return tables[0]["Properties"]["TableInput"]


# --- Glue <-> schema --------------------------------------------------------
@pytest.mark.parametrize(
    "table_name, expected, zone",
    [
        ("vitals_clean", schema.CLEAN_COLUMNS, schema.ZONE_CLEAN),
        ("vitals_quarantine", schema.QUARANTINE_COLUMNS, schema.ZONE_QUARANTINE),
    ],
)
def test_glue_table_matches_contract(resources, analytics_bucket, table_name, expected, zone):
    ti = _glue_table(resources, table_name)
    columns = ti["StorageDescriptor"]["Columns"]

    assert [(c["Name"], c["Type"].lower()) for c in columns] == list(expected)
    assert ti["PartitionKeys"] == [{"Name": "dt", "Type": "string"}]

    params = ti["Parameters"]
    assert params["projection.dt.range"].split(",")[0] == schema.PROJECTION_START
    assert params["projection.dt.format"] == "yyyy-MM-dd"
    assert schema.DT_FORMAT == "%Y-%m-%d"
    # ${!dt} inside !Sub renders as the literal ${dt} Athena expects
    assert params["storage.location.template"] == f"s3://${{{analytics_bucket}}}/{zone}/dt=${{!dt}}/"
    assert ti["StorageDescriptor"]["Location"] == f"s3://${{{analytics_bucket}}}/{zone}/"


# --- PII guarantee ----------------------------------------------------------
def test_nothing_but_the_transform_can_write_the_analytics_bucket(
    resources, transform, analytics_bucket
):
    """Raw records have no path into the analytics bucket."""
    for cfn_type in ("AWS::KinesisFirehose::DeliveryStream", "AWS::IAM::Role",
                     "AWS::IAM::Policy", "AWS::IAM::RolePolicy",
                     "AWS::IAM::ManagedPolicy", "AWS::S3::BucketPolicy"):
        for logical_id, resource in _by_type(resources, cfn_type).items():
            assert analytics_bucket not in repr(resource), logical_id


def test_transform_writes_only_zone_prefixes(transform, analytics_bucket):
    _, props = transform
    statements = [
        stmt
        for policy in props["Policies"]
        if isinstance(policy, dict) and "Statement" in policy
        for stmt in _as_list(policy["Statement"])
    ]
    s3 = [s for s in statements if analytics_bucket in repr(s)]
    assert len(s3) == 1
    assert _as_list(s3[0]["Action"]) == ["s3:PutObject"]
    assert s3[0]["Resource"] == [
        f"${{{analytics_bucket}.Arn}}/{schema.ZONE_CLEAN}/*",
        f"${{{analytics_bucket}.Arn}}/{schema.ZONE_QUARANTINE}/*",
    ]


def test_raw_backup_goes_only_to_raw_bucket(resources, raw_stream, analytics_bucket):
    dest = raw_stream["ExtendedS3DestinationConfiguration"]
    raw_bucket = dest["BucketARN"].removesuffix(".Arn")
    assert resources[raw_bucket]["Type"] == "AWS::S3::Bucket"
    assert raw_bucket != analytics_bucket
    assert "ProcessingConfiguration" not in dest
    assert "S3BackupConfiguration" not in dest


# --- Wiring -----------------------------------------------------------------
def test_event_source_mapping_config(resources, transform, raw_stream):
    _, props = transform
    events = [e for e in props["Events"].values() if e["Type"] == "Kinesis"]
    assert len(events) == 1
    ep = events[0]["Properties"]

    # Same stream the raw backup reads
    assert ep["Stream"] == raw_stream["KinesisStreamSourceConfiguration"]["KinesisStreamARN"]
    assert ep["StartingPosition"] == "TRIM_HORIZON"
    assert ep["BisectBatchOnFunctionError"] is False

    on_failure = ep["DestinationConfig"]["OnFailure"]
    assert on_failure["Type"] == "SQS"
    assert resources[on_failure["Destination"].removesuffix(".Arn")]["Type"] == "AWS::SQS::Queue"


def test_hmac_param_name_consistent(transform):
    _, props = transform
    env_name = props["Environment"]["Variables"]["HMAC_PARAM_NAME"]
    policy_names = [
        p["SSMParameterReadPolicy"]["ParameterName"]
        for p in props["Policies"]
        if isinstance(p, dict) and "SSMParameterReadPolicy" in p
    ]
    assert env_name.startswith("/")
    assert policy_names == [env_name.lstrip("/")]


def test_handler_path_exists(transform):
    _, props = transform
    module, _func = props["Handler"].rsplit(".", 1)
    src = TEMPLATE.parent / props["CodeUri"]
    assert (src / (module.replace(".", "/") + ".py")).is_file()

def test_dedup_view_selects_every_clean_column():
    sql = (TEMPLATE.parent / "sql" / "vitals_dedup.sql").read_text()
    select = sql.split("SELECT", 1)[1].split("FROM", 1)[0]
    columns = [c.strip() for c in select.split(",")]
    assert columns == [name for name, _ in schema.CLEAN_COLUMNS] + ["dt"]