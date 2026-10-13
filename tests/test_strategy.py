import pytest

import strategy as st


def test_body_change_pct():
    assert st.body_change_pct(100, 103) == pytest.approx(3.0)
    assert st.body_change_pct(100, 97) == pytest.approx(-3.0)
    assert st.body_change_pct(0, 1) == 0.0


@pytest.mark.parametrize("o,c,expected", [
    (100, 103, st.LONG),       # justo el 3 % cuenta
    (100, 110, st.LONG),
    (100, 97, st.SHORT),
    (100, 102.99, None),
    (100, 97.01, None),
])
def test_signal_follows_trend(o, c, expected):
    assert st.signal_direction(o, c, 3.0) == expected


def test_take_profit_long_and_short():
    assert st.take_profit_price(100, st.LONG, 2) == pytest.approx(102)
    assert st.take_profit_price(100, st.SHORT, 2) == pytest.approx(98)
    assert not st.take_profit_hit(101.99, 100, st.LONG, 2)
    assert st.take_profit_hit(102, 100, st.LONG, 2)
    assert st.take_profit_hit(105, 100, st.LONG, 2)
    assert not st.take_profit_hit(98.01, 100, st.SHORT, 2)
    assert st.take_profit_hit(98, 100, st.SHORT, 2)
    assert not st.take_profit_hit(0, 100, st.SHORT, 2)


def test_favorable_move_and_pnl():
    assert st.favorable_move_pct(102, 100, st.LONG) == pytest.approx(2)
    assert st.favorable_move_pct(102, 100, st.SHORT) == pytest.approx(-2)
    assert st.pnl_usd(98, 100, 0.05, st.SHORT) == pytest.approx(0.1)
    assert st.pnl_usd(98, 100, 0.05, st.LONG) == pytest.approx(-0.1)


def test_size_order_rounds_up_to_reach_min_notional():
    qty, eff, why = st.size_order(5, 0.3333, step=1, min_notional=5)
    assert why == "" and qty == 16 and eff >= 5


def test_size_order_skips_when_pair_minimum_is_higher():
    qty, _, why = st.size_order(5, 100, step=0.001, min_notional=20)
    assert qty == 0 and "mínimo" in why


def test_size_order_skips_coarse_step_on_expensive_pair():
    qty, _, why = st.size_order(5, 60000, step=0.001, min_notional=5, max_over=1.5)
    assert qty == 0 and "stepSize" in why


def test_size_order_decimal_step():
    qty, eff, why = st.size_order(5, 2.1234, step=0.1, min_notional=5)
    assert why == "" and qty == pytest.approx(2.4) and 5 <= eff < 5.2
