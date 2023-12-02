from dataverse.utils.text import convert_meta_keys_to_title_case


def test_conversion_to_snake(localized_label, label):
    loc = convert_meta_keys_to_title_case(localized_label.__dict__)
    expected_keys = ["Label", "LanguageCode", "@odata.type"]
    assert all([k in loc.keys() for k in expected_keys])
    assert loc["Label"] == "Test"
    assert loc["LanguageCode"] == 69  # Preserve type!
    assert "LocalizedLabel" in loc["@odata.type"]

    lab = convert_meta_keys_to_title_case(label.__dict__)
    expected_keys = ["LocalizedLabels", "@odata.type"]
    assert all([k in lab.keys() for k in expected_keys])
    assert "Label" in lab["@odata.type"]
    assert len(lab["LocalizedLabels"]) == 2

    decoded = label()
    assert decoded == {
        "@odata.type": "Microsoft.Dynamics.CRM.Label",
        "LocalizedLabels": [
            {"@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel", "Label": "Test", "LanguageCode": 69},
            {"@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel", "Label": "Other Label", "LanguageCode": 420},
        ],
    }
