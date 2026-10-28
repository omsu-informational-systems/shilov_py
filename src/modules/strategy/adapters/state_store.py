from collections import OrderedDict

from ..core.entities import Recommendation, TickerState
from ..ports.interfaces import StrategyStateRepositoryPort


class InMemoryStateRepository(StrategyStateRepositoryPort):
    def __init__(self, max_dedup_cache: int = 8192):
        self.processed_events: OrderedDict[str, Recommendation] = OrderedDict()
        self.max_dedup_cache = max_dedup_cache
        # Ключ: (run_id, ticker)
        self.states: dict[tuple[str, str], TickerState] = {}
        # Текущие позиции портфеля по тикеру (портфель живет сквозь запуски - Глава 2)
        self.portfolio_positions: dict[str, int] = {}

    def is_candle_processed(self, event_id: str) -> bool:
        return event_id in self.processed_events

    def mark_candle_processed(
        self, event_id: str, recommendation: Recommendation
    ) -> None:
        self.processed_events[event_id] = recommendation
        if len(self.processed_events) > self.max_dedup_cache:
            self.processed_events.popitem(last=False)

    def get_ticker_state(self, run_id: str, ticker: str) -> TickerState:
        key = (run_id, ticker)
        if key not in self.states:
            self.states[key] = TickerState(
                position_quantity=self.portfolio_positions.get(ticker, 0)
            )
        else:
            self.states[key].position_quantity = self.portfolio_positions.get(ticker, 0)
        return self.states[key]

    def update_position(self, ticker: str, position_quantity: int) -> None:
        self.portfolio_positions[ticker] = position_quantity
