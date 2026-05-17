"""Schema docs for test seeding.

These are the schema_doc JSONB values that would be stored in vendor_schemas.
They encode what the old hardcoded adapters did, expressed as declarative
extraction paths against the raw payload.
"""

MAERSK_SCHEMA_DOC = {
    "classification": "shipment",
    "entity_external_id_template": "{transport_doc.number}:{container}",
    "event_timestamp_path": "$.milestone_at",
    "raw_event_type_path": "$.milestone",
    "reference_ids": {
        "mbl_number": "$.transport_doc.number",
        "container": "$.container",
        "carrier_scac": "$.carrier_scac",
        "vendor_event_id": "$.event_msg_id",
        "shipper_ref": "$.shipper_ref",
    },
    "location": {
        "code": "$.port.code",
        "name": "$.port.name",
    },
}

ONE_SCHEMA_DOC = {
    "classification": "shipment",
    "entity_external_id_template": "{house_bl}:{container_no}",
    "event_timestamp_path": "$.milestone_local_time",
    "raw_event_type_path": "$.milestone_text",
    "reference_ids": {
        "house_bl": "$.house_bl",
        "master_bl": "$.master_bl",
        "container": "$.container_no",
        "consignee": "$.consignee",
        "delivery_order_no": "$.delivery_order_no",
        "carrier_scac": "$.carrier_scac",
        "vendor_event_id": "$.event_id",
    },
    "location": {
        "code": "$.port_of_discharge",
    },
}

GLOBALFREIGHTPAY_SCHEMA_DOC = {
    "classification": "invoice",
    "entity_external_id_path": "$.doc_ref",
    "event_timestamp_paths": [
        "$.transaction.settled_at",
        "$.transaction.issued_at",
        "$.transaction.voided_at",
        "$.transaction.refunded_at",
    ],
    "raw_event_type_path": "$.transaction.kind",
    "amount_path": "$.transaction.amount",
    "due_at_path": "$.transaction.due_at",
    "linked_references": {
        "carrier": "$.carrier",
        "linked_bl": "$.linked_bl",
        "channel": "$.channel",
        "remitter": "$.transaction.remitter",
        "memo": "$.transaction.memo",
    },
}

MARINE_TRAFFIC_SCHEMA_DOC = {
    "classification": "unclassified",
    "summary_path": "$.subject",
    "reason": "marine_traffic_advisory_is_operational_not_shipment_or_invoice",
}

# Event type mappings that the old adapters' keyword logic would produce.
EVENT_TYPE_MAPPINGS = [
    # Maersk shipment events
    ("maersk", "Loaded onboard and sailed", "shipment", "shipment.in_transit"),
    ("maersk", "Empty container released to shipper; full container received at origin terminal", "shipment", "shipment.picked_up"),
    # ONE shipment events
    ("ocean_network_express", "Cargo released to consignee at consignee facility — empty container returned to depot", "shipment", "shipment.delivered"),
    # GFP invoice events
    ("globalfreightpay", "settled in full", "invoice", "invoice.paid"),
    ("globalfreightpay", "freight invoice raised", "invoice", "invoice.issued"),
]
