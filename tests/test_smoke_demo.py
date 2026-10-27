import base64
import json

from scripts.smoke_demo import matching_dead_letter_envelope


def test_matching_dead_letter_envelope_identifies_its_original_payload() -> None:
    raw_payload = b"original-protobuf-bytes"
    envelope = {
        "reason": "semantic_validation_error: event_id must be a valid UUID",
        "raw_payload_base64": base64.b64encode(raw_payload).decode("ascii"),
        "source": {"topic": "telemetry.raw"},
    }

    assert matching_dead_letter_envelope(json.dumps(envelope).encode(), raw_payload) == envelope


def test_matching_dead_letter_envelope_ignores_other_or_non_json_records() -> None:
    assert matching_dead_letter_envelope(b"not json", b"expected") is None
    assert matching_dead_letter_envelope(
        b'{"raw_payload_base64":"b3RoZXI="}', b"expected"
    ) is None
