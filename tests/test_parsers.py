from datetime import UTC, datetime

import pytest

from app.adapters.parsers import parse_money, parse_timestamp


class TestParseMoney:
    def test_european_format(self):
        assert parse_money("EUR 24.350,75") == ("EUR", 2_435_075)

    def test_us_format(self):
        assert parse_money("USD 1,234.56") == ("USD", 123_456)

    def test_dollar_symbol_no_code(self):
        assert parse_money("$1234.50") == ("USD", 123_450)

    def test_euro_symbol(self):
        assert parse_money("€1500,00") == ("EUR", 150_000)

    def test_no_decimals(self):
        assert parse_money("USD 24350") == ("USD", 2_435_000)

    def test_no_decimals_european(self):
        assert parse_money("EUR 21.000,00") == ("EUR", 2_100_000)

    def test_one_decimal(self):
        assert parse_money("USD 5.5") == ("USD", 550)

    def test_pound_symbol(self):
        assert parse_money("£99.99") == ("GBP", 9_999)

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            parse_money("")

    def test_garbage_raises(self):
        with pytest.raises(ValueError):
            parse_money("not a number")


class TestParseTimestamp:
    def test_iso_with_offset(self):
        ts = parse_timestamp("2026-04-21T22:47:00+08:00")
        assert ts == datetime(2026, 4, 21, 14, 47, 0, tzinfo=UTC)

    def test_iso_zulu(self):
        ts = parse_timestamp("2026-04-26T06:00:00Z")
        assert ts == datetime(2026, 4, 26, 6, 0, 0, tzinfo=UTC)

    def test_european_with_tz_alias_wib(self):
        # "28/04/2026 09:42 WIB" — WIB is UTC+7
        ts = parse_timestamp("28/04/2026 09:42 WIB")
        assert ts == datetime(2026, 4, 28, 2, 42, 0, tzinfo=UTC)

    def test_space_separator_with_offset(self):
        ts = parse_timestamp("2026-04-22 18:47:11+02:00")
        assert ts == datetime(2026, 4, 22, 16, 47, 11, tzinfo=UTC)

    def test_naive_defaults_to_utc(self):
        ts = parse_timestamp("2026-04-26T06:00:00")
        assert ts.tzinfo is not None
        assert ts == datetime(2026, 4, 26, 6, 0, 0, tzinfo=UTC)

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            parse_timestamp("")
