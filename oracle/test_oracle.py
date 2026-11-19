"""Unit tests for the oracle, standard library only: python3 -m unittest discover -s oracle"""
import copy
import json
import unittest

import oracle

SUB, OTHER = "+8801012345678", "+8801087654321"


def voice(**changes):
    base = {"event_id": "e1", "event_type": "VOICE", "msisdn": SUB, "counterparty_msisdn": OTHER, "cell_id": "DHK-0001",
            "event_time": "2026-01-01T10:00:00.000Z", "emitted_at": "2026-01-01T10:00:05.000Z",
            "duration_sec": 60, "sms_segments": None, "bytes_up": None, "bytes_down": None, "charge_paisa": 120}
    base.update(changes)
    return json.dumps(base)


def sms(**changes):
    return voice(**{"event_type": "SMS", "duration_sec": None, "sms_segments": 2, "charge_paisa": 100, **changes})


def data(**changes):
    return voice(**{"event_type": "DATA", "counterparty_msisdn": None, "duration_sec": None, "bytes_up": 1000,
                    "bytes_down": 5000, "charge_paisa": 30, **changes})


class ValidityTest(unittest.TestCase):
    def test_each_kind_of_valid_record_is_kept(self):
        events, dropped = oracle.latest_copies([voice(), sms(event_id="e2"), data(event_id="e3")])
        self.assertEqual((len(events), dropped), (3, 0))

    def test_every_rule_drops_the_record(self):
        bad = ["not json", "[]", voice(event_id=None), voice(event_id=""), voice(event_type="FAX"), voice(msisdn="01012345678"),
               voice(counterparty_msisdn=None), sms(counterparty_msisdn="+8801"), voice(cell_id=" "),
               voice(event_id="x+8801012345678"), voice(cell_id="DHK+8801012345678"), voice(event_time="yesterday"),
               voice(emitted_at=None), voice(event_time="2026-01-01T11:00:00.000Z"), voice(duration_sec=-1),
               voice(duration_sec=None), sms(sms_segments=0), data(bytes_up=-1), data(bytes_down=None), voice(charge_paisa=-1)]
        for line in bad:
            with self.subTest(line=line[:70]):
                self.assertEqual(oracle.latest_copies([line]), ([], 1))

    def test_a_data_record_needs_no_counterparty(self):
        self.assertEqual(len(oracle.latest_copies([data(counterparty_msisdn=None)])[0]), 1)

    def test_a_stray_metric_on_another_type_does_not_make_a_record_invalid(self):
        self.assertEqual(len(oracle.latest_copies([data(duration_sec=999, sms_segments=9)])[0]), 1)

    def test_a_complete_json_object_followed_by_text_is_kept_but_a_truncated_one_is_not(self):
        self.assertEqual(oracle.latest_copies([voice() + " trailing text", voice(event_id="e2") + "}{"])[1], 0)
        self.assertEqual(oracle.latest_copies([voice()[:-1]])[1], 1)

    def test_blank_lines_are_not_counted_as_dropped(self):
        self.assertEqual(oracle.latest_copies(["", "  ", voice()])[1], 0)


class DedupeTest(unittest.TestCase):
    def test_the_latest_emitted_copy_wins_in_either_order(self):
        old, new = voice(charge_paisa=100), voice(charge_paisa=150, emitted_at="2026-01-01T10:09:00.000Z")
        for lines in ([old, new], [new, old]):
            self.assertEqual(oracle.expected(lines)["days"]["2026-01-01"]["charge_paisa"], 150)

    def test_a_resend_counts_once(self):
        self.assertEqual(oracle.expected([voice(), voice()])["days"]["2026-01-01"]["events"], 1)

    def test_an_invalid_correction_never_replaces_a_valid_original(self):
        bad_fix = voice(charge_paisa=-5, emitted_at="2026-01-01T10:09:00.000Z")
        self.assertEqual(oracle.expected([voice(), bad_fix])["days"]["2026-01-01"]["charge_paisa"], 120)

    def test_a_correction_that_changes_the_day_moves_the_event(self):
        fix = voice(event_time="2025-12-29T00:00:30.000Z", emitted_at="2026-01-01T10:09:00.000Z", charge_paisa=99)
        want = oracle.expected([voice(), fix])
        self.assertEqual(list(want["days"]), ["2025-12-29"])
        self.assertEqual(want["days"]["2025-12-29"]["charge_paisa"], 99)


class ExpectedTest(unittest.TestCase):
    def test_each_metric_is_summed_over_its_own_type_only(self):
        day = oracle.expected([voice(), voice(event_id="e2", duration_sec=30, charge_paisa=60), sms(event_id="e3"),
                               data(event_id="e4", duration_sec=999, sms_segments=9)])["days"]["2026-01-01"]
        self.assertEqual((day["voice_calls"], day["voice_seconds"]), (2, 90))
        self.assertEqual((day["sms_count"], day["sms_segments"]), (1, 2))
        self.assertEqual((day["data_sessions"], day["data_bytes_up"], day["data_bytes_down"]), (1, 1000, 5000))
        self.assertEqual((day["events"], day["charge_paisa"]), (4, 120 + 60 + 100 + 30))

    def test_days_are_utc_days_with_the_boundary_to_the_second(self):
        want = oracle.expected([voice(event_id="a", event_time="2026-01-01T23:59:59.000Z", emitted_at="2026-01-02T00:00:01.000Z"),
                                voice(event_id="b", event_time="2026-01-02T00:00:00.000Z", emitted_at="2026-01-02T00:00:01.000Z")])
        self.assertEqual(sorted(want["days"]), ["2026-01-01", "2026-01-02"])

    def test_a_timestamp_with_an_offset_is_converted_to_utc(self):
        want = oracle.expected([voice(event_time="2026-01-02T03:00:00.000+06:00", emitted_at="2026-01-02T04:00:00.000+06:00")])
        self.assertEqual(list(want["days"]), ["2026-01-01"])

    def test_subscribers_are_distinct_per_day_and_per_cell(self):
        want = oracle.expected([voice(event_id="a"), sms(event_id="b"), voice(event_id="c", msisdn=OTHER),
                                voice(event_id="d", msisdn=OTHER, cell_id="DHK-0002")])
        self.assertEqual(want["days"]["2026-01-01"]["subscribers"], 2)
        self.assertEqual(want["cells"]["2026-01-01|DHK-0001"]["distinct_subscribers"], 2)
        self.assertEqual(want["cells"]["2026-01-01|DHK-0002"]["distinct_subscribers"], 1)

    def test_revenue_by_type_and_the_three_views_agree_on_total_charge(self):
        want = oracle.expected([voice(), sms(event_id="b"), data(event_id="c", cell_id="DHK-0002")])
        self.assertEqual(sum(v["charge_paisa"] for v in want["days"].values()), 250)
        self.assertEqual(sum(v["charge_paisa"] for v in want["types"].values()), 250)
        self.assertEqual(sum(v["charge_paisa"] for v in want["cells"].values()), 250)
        self.assertEqual(want["types"]["2026-01-01|SMS"], {"events": 1, "charge_paisa": 100})


class DifferencesTest(unittest.TestCase):
    def setUp(self):
        self.want = oracle.expected([voice(), sms(event_id="b"), data(event_id="c", event_time="2026-01-02T09:00:00.000Z",
                                                                      emitted_at="2026-01-02T09:00:01.000Z")])

    def test_identical_values_agree(self):
        self.assertEqual(oracle.differences(self.want, copy.deepcopy(self.want)), [])

    def test_a_changed_number_is_reported_with_where_and_what(self):
        got = copy.deepcopy(self.want)
        got["days"]["2026-01-01"]["voice_seconds"] += 1
        self.assertEqual(oracle.differences(self.want, got), ["days 2026-01-01 voice_seconds: expected 60, gold has 61"])

    def test_a_missing_and_an_extra_row_are_reported(self):
        got = copy.deepcopy(self.want)
        del got["types"]["2026-01-01|SMS"]
        got["cells"]["2026-01-03|DHK-0009"] = {"events": 1}
        found = oracle.differences(self.want, got)
        self.assertEqual(len(found), 2)
        self.assertIn("gold has no row", found[0] + found[1])
        self.assertIn("do not explain", found[0] + found[1])

    def test_cell_volumes_are_expected_per_cell_and_a_volume_in_the_wrong_cell_is_reported(self):
        want = oracle.expected([voice(event_id="a", cell_id="DHK-0001"), sms(event_id="b", cell_id="DHK-0002"),
                                data(event_id="c", cell_id="DHK-0001"), voice(event_id="d", event_time="2026-01-02T09:00:00.000Z",
                                                                              emitted_at="2026-01-02T09:00:01.000Z")])
        self.assertEqual({k: want["cells"]["2026-01-01|DHK-0001"][k] for k in ("voice_seconds", "data_bytes")},
                         {"voice_seconds": 60, "data_bytes": 6000})
        self.assertEqual(want["cells"]["2026-01-01|DHK-0002"]["sms_segments"], 2)
        got = copy.deepcopy(want)
        got["cells"]["2026-01-01|DHK-0001"]["data_bytes"] = 0  # the daily total could still be right in another cell
        got["cells"]["2026-01-01|DHK-0002"]["data_bytes"] = 6000
        found = oracle.differences(want, got)
        self.assertEqual(sorted(found), ["cells 2026-01-01|DHK-0001 data_bytes: expected 6000, gold has 0",
                                         "cells 2026-01-01|DHK-0002 data_bytes: expected 0, gold has 6000"])

    def test_swapped_upload_and_download_totals_are_reported_even_though_their_sum_is_unchanged(self):
        got = copy.deepcopy(self.want)
        got["days"]["2026-01-02"]["data_bytes_up"], got["days"]["2026-01-02"]["data_bytes_down"] = 5000, 1000
        self.assertEqual(sorted(oracle.differences(self.want, got)),
                         ["days 2026-01-02 data_bytes_down: expected 5000, gold has 1000",
                          "days 2026-01-02 data_bytes_up: expected 1000, gold has 5000"])

    def test_a_key_gold_holds_twice_is_reported(self):
        got = copy.deepcopy(self.want)
        got["duplicate_keys"] = ["cell_traffic 2026-01-01|DHK-0001"]
        self.assertEqual(oracle.differences(self.want, got), ["gold holds more than one row for cell_traffic 2026-01-01|DHK-0001"])

    def test_a_zero_gold_leaves_out_or_writes_as_zero_agrees_but_a_missing_nonzero_does_not(self):
        got = copy.deepcopy(self.want)
        got["days"]["2026-01-02"]["voice_seconds"] = 0  # the oracle has no such key for a day without voice
        self.assertEqual(oracle.differences(self.want, got), [])
        del got["days"]["2026-01-01"]["voice_seconds"]
        self.assertEqual(len(oracle.differences(self.want, got)), 1)


if __name__ == "__main__":
    unittest.main()
