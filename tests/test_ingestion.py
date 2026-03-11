from src.ingestion.tlc_ingestion import _month_range


def test_month_range_inclusive():
    assert _month_range("2024-01", "2024-03") == ["2024-01", "2024-02", "2024-03"]
