import src.utils.spark_utils as spark_utils


class _FakeConf:
    def __init__(self, values):
        self._values = values

    def get(self, key, default=""):
        return self._values.get(key, default)


class _FakeSpark:
    def __init__(self, values):
        self.conf = _FakeConf(values)
        self.stopped = False

    def stop(self):
        self.stopped = True


def test_is_delta_session_configured_true():
    spark = _FakeSpark(
        {
            "spark.sql.extensions": spark_utils.DELTA_EXTENSION,
            "spark.sql.catalog.spark_catalog": spark_utils.DELTA_CATALOG,
        }
    )
    assert spark_utils.is_delta_session_configured(spark) is True


def test_ensure_delta_spark_session_reuses_active(monkeypatch):
    active = _FakeSpark(
        {
            "spark.sql.extensions": spark_utils.DELTA_EXTENSION,
            "spark.sql.catalog.spark_catalog": spark_utils.DELTA_CATALOG,
        }
    )

    monkeypatch.setattr(spark_utils.SparkSession, "getActiveSession", staticmethod(lambda: active))
    result = spark_utils.ensure_delta_spark_session("app")
    assert result is active


def test_ensure_delta_spark_session_rebuilds_non_delta(monkeypatch):
    active = _FakeSpark({})
    created = object()

    monkeypatch.setattr(spark_utils.SparkSession, "getActiveSession", staticmethod(lambda: active))

    class _Cfg:
        def __init__(self, _builder):
            pass

        def getOrCreate(self):
            return created

    monkeypatch.setattr(spark_utils, "configure_spark_with_delta_pip", lambda builder: _Cfg(builder))

    result = spark_utils.ensure_delta_spark_session("app")
    assert active.stopped is True
    assert result is created
