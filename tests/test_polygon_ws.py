from market_streaming.producer.polygon_ws import build_subscription_params


def test_default_channels_minute_aggregates_only():
    params = build_subscription_params(["AAPL", "MSFT"])
    assert params.split(",") == ["AM.AAPL", "AM.MSFT"]


def test_explicit_channel_list_supports_future_t_q_upgrade():
    params = build_subscription_params(["AAPL"], channels=["AM", "T", "Q"])
    assert params.split(",") == ["AM.AAPL", "T.AAPL", "Q.AAPL"]


def test_build_subscription_params_empty_symbols():
    assert build_subscription_params([]) == ""
