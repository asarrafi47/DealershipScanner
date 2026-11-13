"""Scan-loop tests: scrubber (card L5). Shard-merge tests (card L6) follow below."""

from __future__ import annotations

import copy
import gzip
import itertools
import json
import secrets
import string
import time
from pathlib import Path

import pytest

from backend.scanner.loop import scrub
from backend.scanner.loop.bundle import load_shard, merge_shards
from backend.scanner.loop.scrub import assert_clean, clean_line, scrub_json, scrub_text, secret_values

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "ops_publish"
SHARD_NAMES = ("scan_2026_11_03_s0", "scan_2026_11_03_s3", "scan_2026_11_03_s6")


def _rand(n: int = 24) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(n))


@pytest.fixture()
def env() -> dict:
    return {
        "TEST_API_KEY": _rand(),
        "SOME_service_token": _rand(30),
        "DB_PASSWORD": _rand(16),
        "MY_SECRET": _rand(12),
        "SEARCH_URL": "https://search.example/" + _rand(10),
        "RAILWAY_PROJECT_ID": _rand(20),
        "RAILWAY_PUBLIC_DOMAIN": _rand(10) + ".example",
        "RAILWAY_ENVIRONMENT_NAME": "production-env-name",
        "SHORT_KEY": "1",
        "FLAG_SECRET": "true",
        "PLAIN": "ordinary-value-here",
    }


# --- secret_values ---------------------------------------------------------------------------------


def test_secret_values_picks_names_and_ignores_short_values(env) -> None:
    values = secret_values(env)
    for name in ("TEST_API_KEY", "SOME_service_token", "DB_PASSWORD", "MY_SECRET", "SEARCH_URL"):
        assert env[name] in values
    assert env["RAILWAY_PROJECT_ID"] in values
    assert env["RAILWAY_PUBLIC_DOMAIN"] in values
    assert "1" not in values and "true" not in values
    assert "production-env-name" not in values
    assert "ordinary-value-here" not in values


def test_secret_values_longest_first_and_deduplicated() -> None:
    values = secret_values({"A_KEY": "x" * 9, "B_KEY": "y" * 20, "C_KEY": "x" * 9})
    assert values == ["y" * 20, "x" * 9]


def test_secret_values_url_with_credentials_adds_password_and_host() -> None:
    env = {"WHATEVER": "postgres://appuser:Pa55word99@db.internal.example:5432/app"}
    values = secret_values(env)
    assert "postgres://appuser:Pa55word99@db.internal.example:5432/app" in values
    assert "Pa55word99" in values
    assert "db.internal.example" in values


def test_secret_values_loopback_host_is_not_a_secret() -> None:
    env = {"WHATEVER": "postgres://appuser:Pa55word99@localhost:5432/app", "OTHER": "redis://u:Pa55word88@127.0.0.1/0"}
    values = secret_values(env)
    assert "Pa55word99" in values and "Pa55word88" in values
    assert "localhost" not in values and "127.0.0.1" not in values


def test_secret_values_ignores_non_string_entries() -> None:
    assert secret_values({"A_KEY": 123456789, 5: "abcdefghij", "B_KEY": None}) == []


# --- scrub_text ------------------------------------------------------------------------------------


def test_scrub_text_removes_planted_values_everywhere_and_counts(env) -> None:
    key = env["TEST_API_KEY"]
    text = f"start {key} mid https://x.example/p?q={key} end {key}"
    out, counts = scrub_text(text, env)
    assert key not in out
    assert out.count(scrub.ENV_MARK) == 3
    assert counts["env_value"] == 3
    assert counts["url_param"] == 0 and counts["header_value"] == 0 and counts["control_chars"] == 0


def test_scrub_text_replaces_longest_value_first() -> None:
    long_value = "abcdefgh12345678"
    short_value = "abcdefgh"
    out, counts = scrub_text(f"x {long_value} y {short_value}", {"A_KEY": long_value, "B_KEY": short_value})
    assert out == "x [redacted:env] y [redacted:env]"
    assert counts["env_value"] == 2


def test_scrub_text_matches_regex_metacharacters_literally() -> None:
    value = "a.b*c+d?e(f)[g]{h}|^$\\"
    out, _ = scrub_text("before " + value + " after aXbbcdde", {"X_SECRET": value})
    assert out == "before [redacted:env] after aXbbcdde"


def test_scrub_text_blanks_key_like_query_parameters_only() -> None:
    url = (
        "https://api.example/s?apikey=AAA111&x-algolia-api-key=BBB222&access_token=CCC333"
        "&keyword=suv&x-algolia-application-id=APP123&q=civic"
    )
    out, counts = scrub_text(url, {})
    assert "AAA111" not in out and "BBB222" not in out and "CCC333" not in out
    assert "keyword=suv" in out and "x-algolia-application-id=APP123" in out and "q=civic" in out
    assert counts["url_param"] == 3


@pytest.mark.parametrize(
    "name", ["key", "token", "secret", "password", "passwd", "pwd", "auth", "sig", "signature", "session", "sessionid"]
)
def test_scrub_text_param_names_in_the_rule(name: str) -> None:
    out, counts = scrub_text("http://h.example/p?" + name + "=VALUE99", {})
    assert "VALUE99" not in out and counts["url_param"] == 1


@pytest.mark.parametrize("name", ["keyword", "monkey", "turkey", "tokens", "authority", "pageSize"])
def test_scrub_text_param_names_outside_the_rule(name: str) -> None:
    text = "http://h.example/p?" + name + "=VALUE99"
    out, counts = scrub_text(text, {})
    assert out == text and counts["url_param"] == 0


def test_scrub_text_removes_url_userinfo() -> None:
    out, counts = scrub_text("clone https://someone:s3cr3tpass@host.example/repo.git now", {})
    assert out == "clone https://host.example/repo.git now"
    assert counts["url_param"] == 1
    out2, _ = scrub_text("https://tokenonly@host.example/x", {})
    assert out2 == "https://host.example/x"


def test_scrub_text_blanks_header_like_pairs() -> None:
    text = (
        "Authorization: Bearer abc123def456\n"
        'Cookie: sid=111; other=222\n'
        "Set-Cookie: session=333; Path=/\n"
        "x-algolia-api-key: 444key\n"
        '{"X-Api-Key": "555key", "Authorization":"Basic 666"}\n'
        "x-api-key=777key&next=1\n"
    )
    out, counts = scrub_text(text, {})
    for leaked in ("abc123def456", "sid=111", "session=333", "444key", "555key", "666", "777key"):
        assert leaked not in out
    assert "next=1" in out and "Path=/" not in out
    assert counts["header_value"] == 7


def test_scrub_text_leaves_ordinary_words_alone() -> None:
    text = "cookies: 3 of them; the authority approved; monkey-key lookup; x-ray: fine"
    out, counts = scrub_text(text, {})
    assert out == text
    assert sum(counts.values()) == 0


def test_scrub_text_drops_controls_ansi_bidi_and_zero_width() -> None:
    text = "a\x00b\x1b[31;1mred\x1b[0m\x1b]0;title\x07c‮d​e⁦f﻿g\r\nh\ti\x7f"
    out, counts = scrub_text(text, {})
    assert out == "abredcdefg\nh\ti"
    assert counts["control_chars"] >= 10


def test_scrub_text_removes_a_secret_hidden_by_zero_width_characters() -> None:
    key = _rand()
    hostile = key[:10] + "​" + key[10:]
    out, _ = scrub_text(hostile, {"X_API_KEY": key})
    assert key not in out and "[redacted:env]" in out


def test_scrub_text_is_idempotent(env) -> None:
    key = env["TEST_API_KEY"]
    text = (
        f"{key} https://u:pw@h.example/p?apikey=ZZZ111&keyword=a Authorization: Bearer q1\n"
        '{"cookie":"x=1"} \x1b[1mbold\x1b[0m x-api-key=abc'
    )
    once, _ = scrub_text(text, env)
    twice, counts = scrub_text(once, env)
    assert twice == once
    assert sum(counts.values()) == 0


def test_scrub_text_non_string_comes_back_unchanged() -> None:
    marker = object()
    for value in (None, 5, 1.5, True, b"bytes", [1], {"a": 1}, marker):
        out, counts = scrub_text(value, {"A_KEY": "abcdefghij"})
        assert out is value
        assert counts == {"env_value": 0, "url_param": 0, "header_value": 0, "control_chars": 0}


def test_scrub_text_large_key_equals_input_is_fast() -> None:
    text = "key=" * 262144  # 1 MiB
    assert len(text) == 1024 * 1024
    start = time.perf_counter()
    out, _ = scrub_text(text, {"A_KEY": "abcdefghij"})
    assert time.perf_counter() - start < 2.0
    assert out == text
    start = time.perf_counter()
    scrub_text("https://h.example/?" + text, {})
    scrub_text("Authorization " * 80000, {})
    assert time.perf_counter() - start < 2.0


def test_scrub_text_a_value_equal_to_a_marker_word_does_not_loop() -> None:
    env = {"A_KEY": "redacted"}
    once, _ = scrub_text("x [redacted:env] y", env)
    assert once == "x [redacted:env] y"


# --- clean_line ------------------------------------------------------------------------------------


def test_clean_line_hostile_reason_becomes_one_short_line() -> None:
    hostile = "line1\n\n\tline2\r\n\x1b[31mred\x1b[0m‮" + "A" * 10000 + "\nlast"
    out = clean_line(hostile, 160)
    assert len(out) <= 160
    assert out.endswith("...")
    assert "\n" not in out and "\t" not in out and "\x1b" not in out and "‮" not in out
    assert out.startswith("line1 line2 red")


def test_clean_line_short_text_is_only_normalised() -> None:
    assert clean_line("  a \n\n b\t\tc  ", 160) == "a b c"
    assert clean_line("", 10) == ""
    assert clean_line("x" * 10, 10) == "x" * 10
    assert clean_line("x" * 11, 10) == "xxxxxxx..."


def test_clean_line_small_limits_and_non_strings() -> None:
    assert clean_line("abcdef", 3) == "abc"
    assert clean_line("abcdef", 0) == ""
    assert clean_line(None, 10) is None  # type: ignore[arg-type]


# --- scrub_json ------------------------------------------------------------------------------------


def test_scrub_json_cleans_nested_strings_and_counts(env) -> None:
    key = env["TEST_API_KEY"]
    obj = {
        "a": f"v {key}",
        "list": [f"https://h.example/?token={_rand(10)}", 5, None, True, 1.5, ["deep " + key]],
        "nested": {"k": {"x": "Authorization: Bearer abc"}, "n": 7},
        "t": ("tuple", key),
    }
    snapshot = copy.deepcopy(obj)
    out, counts = scrub_json(obj, env)
    assert obj == snapshot
    assert key not in json.dumps(out)
    assert out["list"][1:5] == [5, None, True, 1.5]
    assert out["nested"]["n"] == 7
    assert out["t"] == ("tuple", "[redacted:env]")
    assert counts["env_value"] == 3
    assert counts["url_param"] == 1
    assert counts["header_value"] == 1


def test_scrub_json_scrubs_dictionary_keys() -> None:
    key = _rand()
    out, counts = scrub_json({key: "v"}, {"X_API_KEY": key})
    assert list(out) == ["[redacted:env]"]
    assert counts["env_value"] == 1


def test_scrub_json_cuts_long_strings_at_500_and_is_idempotent() -> None:
    out, _ = scrub_json({"s": "z" * 2000, "short": "ok"}, {})
    assert len(out["s"]) == 500 and out["s"].endswith("...")
    assert out["short"] == "ok"
    again, _ = scrub_json(out, {})
    assert again == out


def test_scrub_json_secret_straddling_the_cut_is_removed_first() -> None:
    key = _rand(30)
    text = "q" * 480 + key + "tail"
    out, _ = scrub_json([text], {"X_API_KEY": key})
    assert key not in out[0] and key[:15] not in out[0]


def test_scrub_json_non_container_input() -> None:
    assert scrub_json(5, {})[0] == 5
    assert scrub_json(None, {})[0] is None
    out, counts = scrub_json("plain", {})
    assert out == "plain" and sum(counts.values()) == 0


def test_scrub_json_twice_equals_once(env) -> None:
    obj = {"a": [env["MY_SECRET"] + " https://u:p@h.example/?sig=abc", {"b": "Cookie: a=b"}]}
    once, _ = scrub_json(obj, env)
    twice, counts = scrub_json(once, env)
    assert twice == once
    assert sum(counts.values()) == 0


# --- assert_clean ----------------------------------------------------------------------------------


def test_assert_clean_passes_on_scrubbed_output(env) -> None:
    out, _ = scrub_text(f"{env['TEST_API_KEY']} https://h.example/?apikey=abc123", env)
    assert_clean([out.encode()], env)
    assert_clean({"a.txt": out.encode(), "b.txt": b"nothing here"}, env)


def test_assert_clean_raises_on_exact_secret_without_leaking_it(env) -> None:
    key = env["TEST_API_KEY"]
    with pytest.raises(ValueError) as excinfo:
        assert_clean({"digest.md": ("fine " + key).encode()}, env)
    message = str(excinfo.value)
    assert "digest.md" in message
    for value in secret_values(env):
        assert value not in message


def test_assert_clean_raises_on_key_like_query_parameter() -> None:
    with pytest.raises(ValueError) as excinfo:
        assert_clean([b"see https://h.example/p?apikey=SUPERVALUE1"], {})
    assert "SUPERVALUE1" not in str(excinfo.value)
    assert_clean([b"see https://h.example/p?apikey=[redacted:param]&keyword=SUPERVALUE1"], {})


def test_assert_clean_accepts_text_and_invalid_utf8() -> None:
    assert_clean(["plain text", b"\xff\xfe binary"], {})


def test_no_message_or_exception_text_contains_a_matched_value(env) -> None:
    key = env["TEST_API_KEY"]
    caught = []
    for blob in (key.encode(), ("x" + key).encode()):
        try:
            assert_clean([blob], env)
        except ValueError as exc:
            caught.append(repr(exc) + str(exc.args))
    assert caught
    assert all(key not in c for c in caught)
    assert "logging" not in Path(scrub.__file__).read_text(encoding="utf-8").split("import", 1)[1][:400]


def test_scrub_module_is_pure_standard_library() -> None:
    import ast

    tree = ast.parse(Path(scrub.__file__).read_text(encoding="utf-8"))
    imported = set()
    attrs = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add((node.module or "").split(".")[0])
        elif isinstance(node, ast.Attribute):
            attrs.add(node.attr)
    assert imported <= {"__future__", "re", "unicodedata", "urllib"}
    assert not attrs & {"environ", "getenv", "now", "utcnow", "open", "read_text"}


# --- fixtures --------------------------------------------------------------------------------------


@pytest.mark.parametrize("shard", SHARD_NAMES)
def test_fixture_shards_produce_zero_redactions(shard: str, env) -> None:
    directory = FIXTURES / shard
    texts = {
        "triage": gzip.open(directory / "triage.json.gz", "rt", encoding="utf-8").read(),
        "needs": (directory / "needs_discovery.txt").read_text(encoding="utf-8"),
        "slow": (directory / "slow_dealers.txt").read_text(encoding="utf-8"),
    }
    for label, text in texts.items():
        out, counts = scrub_text(text, env)
        assert out == text, label
        assert sum(counts.values()) == 0, label
    parsed, counts = scrub_json(json.loads(texts["triage"]), env)
    assert sum(counts.values()) == 0
    assert_clean([t.encode() for t in texts.values()], env)


# =================================================================================================
# Shard merge (card L6)
# =================================================================================================


def _load_fixture_shards() -> list:
    return [load_shard(FIXTURES / name) for name in SHARD_NAMES]


def _write_shard(directory: Path, triage: dict, *, gz: bool, indent=None) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    data = json.dumps(triage, indent=indent, sort_keys=True, separators=(",", ": ") if indent else (",", ":"))
    if gz:
        (directory / "triage.json.gz").write_bytes(gzip.compress(data.encode("utf-8")))
    else:
        (directory / "triage.json").write_text(data, encoding="utf-8")
    return directory


def _fixture_triage(name: str) -> dict:
    with gzip.open(FIXTURES / name / "triage.json.gz", "rt", encoding="utf-8") as handle:
        return json.load(handle)


def test_merge_three_fixture_shards() -> None:
    merged = merge_shards(_load_fixture_shards())
    assert len(merged["dealers"]) == 172
    assert merged["counts"] == {"ok": 101, "thin": 13, "inaccurate": 55, "no_rows": 3, "error": 0, "no_recipe": 0}
    assert merged["needs_discovery"] == ["autosavvy-com", "mckennasubaru-com", "overdrive-usa-com"]
    ids = [row["dealer_id"] for row in merged["dealers"]]
    assert ids == sorted(ids) and len(set(ids)) == 172
    assert len(merged["thin"]) == 13 and len(merged["inaccurate"]) == 55
    assert merged["thin"] == sorted(merged["thin"]) and merged["inaccurate"] == sorted(merged["inaccurate"])
    assert merged["recipe_coverage"] == {"dealers": 172, "with_recipe": 172}
    assert merged["started"] == "2026-11-03T17:59:55+00:00"
    assert merged["platform_clusters"] == []


def test_merge_counts_agree_with_dealer_verdicts_in_fixtures() -> None:
    merged = merge_shards(_load_fixture_shards())
    by_verdict: dict = {}
    for row in merged["dealers"]:
        by_verdict[row["verdict"]] = by_verdict.get(row["verdict"], 0) + 1
    for key in ("ok", "thin", "inaccurate", "no_rows"):
        assert by_verdict.get(key, 0) == merged["counts"][key]


def test_merge_lifecycle_and_text_lists() -> None:
    shards = _load_fixture_shards()
    merged = merge_shards(shards)
    life = merged["lifecycle"]
    assert life["routed"] == 9 and life["resynth_ok"] == 7 and life["capture_ok"] == 1
    assert life["failed"] == 1 and life["resynth_failed"] == 0
    assert len(life["retried"]) == 8 and list(life["retried"]) == sorted(life["retried"])
    assert len(life["by_dealer"]) == 172
    assert merged["needs_discovery_lines"] == sorted(merged["needs_discovery_lines"])
    assert len(merged["needs_discovery_lines"]) == 3
    assert merged["slow_dealers_lines"] == sorted(merged["slow_dealers_lines"])
    assert len(merged["slow_dealers_lines"]) == sum(len(s["slow_dealers_lines"]) for s in shards)


def test_merge_drops_chromium_leaks_and_shard_specific_file_name() -> None:
    merged = merge_shards(_load_fixture_shards())
    text = json.dumps(merged)
    assert "chromium_leaks" not in text and "needs_discovery_file" not in text
    assert "chromium_leaks" not in merged and "chromium_leaks" not in merged["lifecycle"]
    assert "needs_discovery_file" not in merged["lifecycle"]


def test_merge_drops_chromium_leaks_from_indent_one_triage(tmp_path: Path) -> None:
    triage = _fixture_triage(SHARD_NAMES[0])
    triage["chromium_leaks"] = [{"dealer_id": "x"}]
    triage["lifecycle"]["chromium_leaks"] = ["leak"]
    directory = _write_shard(tmp_path / "real", triage, gz=False, indent=1)
    merged = merge_shards([load_shard(directory)])
    assert "chromium_leaks" not in json.dumps(merged)


def test_merge_all_shard_orders_give_equal_output() -> None:
    shards = _load_fixture_shards()
    reference = json.dumps(merge_shards(shards), sort_keys=False)
    for order in itertools.permutations(shards):
        assert json.dumps(merge_shards(list(order)), sort_keys=False) == reference


def test_merge_same_shard_twice_raises_naming_the_ids() -> None:
    shards = _load_fixture_shards()
    with pytest.raises(ValueError) as excinfo:
        merge_shards([shards[0], shards[0]])
    message = str(excinfo.value)
    assert "5starford-com" in message and "mckennasubaru-com" in message


def test_merge_one_dealer_in_two_shards_raises_naming_it() -> None:
    first = _load_fixture_shards()
    clash = copy.deepcopy(first[1])
    clash["triage"]["dealers"].append(copy.deepcopy(first[0]["triage"]["dealers"][0]))
    with pytest.raises(ValueError) as excinfo:
        merge_shards([first[0], clash])
    assert "5starford-com" in str(excinfo.value)
    assert "autosavvy-com" not in str(excinfo.value)


def test_load_shard_missing_triage_raises_with_path(tmp_path: Path) -> None:
    empty = tmp_path / "nothing_here"
    empty.mkdir()
    with pytest.raises(FileNotFoundError) as excinfo:
        load_shard(empty)
    assert str(empty) in str(excinfo.value)
    with pytest.raises(FileNotFoundError):
        load_shard(tmp_path / "does_not_exist")


@pytest.mark.parametrize(
    "payload",
    [b"{not json", b"", b"[1, 2, 3]", b'{"dealers": "nope"}', b'{"counts": {}}', b"\xff\xfe\x00"],
)
def test_load_shard_malformed_triage_raises_with_path(tmp_path: Path, payload: bytes) -> None:
    directory = tmp_path / "bad"
    directory.mkdir()
    (directory / "triage.json").write_bytes(payload)
    with pytest.raises(ValueError) as excinfo:
        load_shard(directory)
    assert str(directory / "triage.json") in str(excinfo.value)


def test_load_shard_malformed_gzip_raises_with_path(tmp_path: Path) -> None:
    directory = tmp_path / "badgz"
    directory.mkdir()
    (directory / "triage.json.gz").write_bytes(b"this is not gzip data")
    with pytest.raises(ValueError) as excinfo:
        load_shard(directory)
    assert str(directory / "triage.json.gz") in str(excinfo.value)
    (directory / "triage.json.gz").write_bytes(gzip.compress(b'{"dealers": []}')[:-6])
    with pytest.raises(ValueError):
        load_shard(directory)


def test_merge_rejects_rows_without_dealer_id_and_unloaded_input() -> None:
    shard = _load_fixture_shards()[0]
    broken = copy.deepcopy(shard)
    del broken["triage"]["dealers"][0]["dealer_id"]
    with pytest.raises(ValueError):
        merge_shards([broken])
    with pytest.raises(ValueError):
        merge_shards([])
    with pytest.raises(ValueError):
        merge_shards([{"not": "a shard"}])


def test_json_and_json_gz_give_identical_results(tmp_path: Path) -> None:
    gz_dirs = []
    plain_dirs = []
    for name in SHARD_NAMES:
        triage = _fixture_triage(name)
        gz_dirs.append(_write_shard(tmp_path / "gz" / name, triage, gz=True))
        plain_dirs.append(_write_shard(tmp_path / "plain" / name, triage, gz=False, indent=1))
    from_gz = merge_shards([load_shard(d) for d in gz_dirs])
    from_plain = merge_shards([load_shard(d) for d in plain_dirs])
    from_fixture = merge_shards(_load_fixture_shards())
    assert from_gz == from_plain == {**from_fixture, "needs_discovery_lines": [], "slow_dealers_lines": []}


def test_merge_does_not_mutate_inputs() -> None:
    shards = _load_fixture_shards()
    snapshot = copy.deepcopy(shards)
    merged = merge_shards(shards)
    assert shards == snapshot
    merged["dealers"][0]["verdict"] = "changed"
    merged["lifecycle"]["retried"]["x"] = "y"
    merged["counts"]["ok"] = -1
    assert shards == snapshot


def test_merge_keeps_dealer_rows_verbatim_including_absent_rows_stamped() -> None:
    shards = _load_fixture_shards()
    originals = {r["dealer_id"]: r for s in shards for r in s["triage"]["dealers"]}
    merged = merge_shards(shards)
    missing = [r["dealer_id"] for r in merged["dealers"] if "rows_stamped" not in r]
    assert len(missing) == 2
    for row in merged["dealers"]:
        assert row == originals[row["dealer_id"]]
        assert isinstance(row["run_id"], int)


def test_merge_without_optional_files_or_log_files(tmp_path: Path) -> None:
    directory = _write_shard(tmp_path / "only_triage", _fixture_triage(SHARD_NAMES[0]), gz=True)
    shard = load_shard(directory)
    assert shard["needs_discovery_lines"] == [] and shard["slow_dealers_lines"] == []
    merged = merge_shards([shard])
    assert len(merged["dealers"]) == 58 and merged["needs_discovery_lines"] == []


def test_merge_started_is_the_earliest_instant() -> None:
    a = {"name": "a", "triage": {"started": "2026-11-03T20:00:00+00:00", "dealers": []}}
    b = {"name": "b", "triage": {"started": "2026-11-03T14:30:00-05:00", "dealers": []}}  # 19:30 UTC
    c = {"name": "c", "triage": {"dealers": []}}
    assert merge_shards([a, b, c])["started"] == "2026-11-03T14:30:00-05:00"
    assert merge_shards([c])["started"] is None


def test_merge_platform_clusters_are_deduplicated_and_ordered() -> None:
    one = {"name": "a", "triage": {"dealers": [], "platform_clusters": [{"platform": "p1", "dealers": ["x"]}, "solo"]}}
    two = {"name": "b", "triage": {"dealers": [], "platform_clusters": ["solo", {"dealers": ["x"], "platform": "p1"}]}}
    merged = merge_shards([one, two])
    assert len(merged["platform_clusters"]) == 2
    assert merged["platform_clusters"] == merge_shards([two, one])["platform_clusters"]


def test_loader_and_merger_module_is_standard_library_only() -> None:
    import ast

    from backend.scanner.loop import bundle

    tree = ast.parse(Path(bundle.__file__).read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add((node.module or "").split(".")[0])
    assert imported <= {"__future__", "copy", "gzip", "json", "datetime", "pathlib"}
