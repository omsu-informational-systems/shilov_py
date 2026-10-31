import uuid
from decimal import Decimal

from .entities import Candle, DataQuality, Recommendation, Signal, TickerState


def calculate_momentum(
    candle: Candle,
    state: TickerState,
    window_size: int = 5,
    threshold_pct: Decimal = Decimal(
        "0.0015"
    ),  # Порог 0.15%, чтобы не торговать рыночный шум и комиссию
) -> Recommendation:
    rec_id = str(uuid.uuid4())
    meta = {"window": window_size}

    # 1. Проверка порядка свечей (Глава 10: при нарушении порядка отвечаем DEGRADED и HOLD)
    if (
        state.last_sequence_number is not None
        and candle.sequence_number <= state.last_sequence_number
    ):
        return Recommendation(
            recommendation_id=rec_id,
            source_event_id=candle.event_id,
            correlation_id=candle.correlation_id,
            run_id=candle.run_id,
            instrument=candle.ticker,
            sequence_number=candle.sequence_number,
            signal=Signal.HOLD,
            confidence=0.0,
            suggested_quantity=0,
            data_quality=DataQuality.DEGRADED,
            reason=f"Нарушение порядка срезов: получен {candle.sequence_number} после {state.last_sequence_number}.",
            processing_time_ms=0,
            metadata=meta,
        )

    data_quality = DataQuality.OK
    if (
        state.last_sequence_number is not None
        and candle.sequence_number > state.last_sequence_number + 1
    ):
        # Возможен пропуск свечи у источника или потеря в сети
        data_quality = DataQuality.DEGRADED

    # Обновляем историю цен в состоянии тикера
    state.last_sequence_number = candle.sequence_number
    state.close_prices.append(candle.close_price)
    if len(state.close_prices) > window_size + 1:
        state.close_prices.pop(0)

    # 2. Обработка фазы закрытия внутридневных позиций FLATTEN (Главы 7, 8)
    if candle.session_phase == "FLATTEN":
        if state.position_quantity > 0:
            lots = max(1, state.position_quantity // max(1, candle.lot_size))
            return Recommendation(
                recommendation_id=rec_id,
                source_event_id=candle.event_id,
                correlation_id=candle.correlation_id,
                run_id=candle.run_id,
                instrument=candle.ticker,
                sequence_number=candle.sequence_number,
                signal=Signal.SELL,
                confidence=0.9,
                suggested_quantity=lots,
                data_quality=data_quality,
                reason="Закрытие длинной позиции в фазе FLATTEN.",
                processing_time_ms=0,
                metadata={"positionEffect": "CLOSE", "targetPosition": 0},
            )
        elif state.position_quantity < 0:
            lots = max(1, abs(state.position_quantity) // max(1, candle.lot_size))
            return Recommendation(
                recommendation_id=rec_id,
                source_event_id=candle.event_id,
                correlation_id=candle.correlation_id,
                run_id=candle.run_id,
                instrument=candle.ticker,
                sequence_number=candle.sequence_number,
                signal=Signal.BUY,
                confidence=0.9,
                suggested_quantity=lots,
                data_quality=data_quality,
                reason="Закрытие короткой позиции в фазе FLATTEN.",
                processing_time_ms=0,
                metadata={"positionEffect": "CLOSE", "targetPosition": 0},
            )

    # 3. Вне основной сессии или в режиме прогрева сделок не открываем (Глава 7, 8)
    if candle.is_warmup or candle.session_phase in (
        "OUTSIDE_MAIN_SESSION",
        "MAIN_SESSION_WARMUP",
    ):
        return Recommendation(
            recommendation_id=rec_id,
            source_event_id=candle.event_id,
            correlation_id=candle.correlation_id,
            run_id=candle.run_id,
            instrument=candle.ticker,
            sequence_number=candle.sequence_number,
            signal=Signal.HOLD,
            confidence=0.0,
            suggested_quantity=0,
            data_quality=data_quality,
            reason=f"Пропуск торговли: фаза {candle.session_phase}, warmup={candle.is_warmup}.",
            processing_time_ms=0,
            metadata=meta,
        )

    # 4. Накопление окна для расчета Momentum (Прогрев алгоритма)
    if len(state.close_prices) <= window_size:
        return Recommendation(
            recommendation_id=rec_id,
            source_event_id=candle.event_id,
            correlation_id=candle.correlation_id,
            run_id=candle.run_id,
            instrument=candle.ticker,
            sequence_number=candle.sequence_number,
            signal=Signal.HOLD,
            confidence=0.0,
            suggested_quantity=0,
            data_quality=data_quality,
            reason=f"Накопление окна данных ({len(state.close_prices)}/{window_size + 1}).",
            processing_time_ms=0,
            metadata=meta,
        )

    # 5. Расчет импульса (Momentum)
    base_price = state.close_prices[0]
    if base_price <= Decimal("0"):
        momentum = Decimal("0")
    else:
        momentum = (candle.close_price - base_price) / base_price

    meta["momentumPct"] = str(round(momentum * Decimal("100"), 4))

    # Сигнал BUY при пробое верхнего порога
    if momentum > threshold_pct:
        # Масштабируем уверенность от 0.55 до 0.95 в зависимости от силы импульса
        raw_conf = float(
            min(Decimal("0.95"), Decimal("0.55") + abs(momentum) * Decimal("20"))
        )
        return Recommendation(
            recommendation_id=rec_id,
            source_event_id=candle.event_id,
            correlation_id=candle.correlation_id,
            run_id=candle.run_id,
            instrument=candle.ticker,
            sequence_number=candle.sequence_number,
            signal=Signal.BUY,
            confidence=round(raw_conf, 2),
            suggested_quantity=1,
            data_quality=data_quality,
            reason=f"Положительный импульс +{meta['momentumPct']}% за {window_size} свечей.",
            processing_time_ms=0,
            metadata=meta,
        )

    # Сигнал SELL при пробое нижнего порога
    if momentum < -threshold_pct:
        raw_conf = float(
            min(Decimal("0.95"), Decimal("0.55") + abs(momentum) * Decimal("20"))
        )
        return Recommendation(
            recommendation_id=rec_id,
            source_event_id=candle.event_id,
            correlation_id=candle.correlation_id,
            run_id=candle.run_id,
            instrument=candle.ticker,
            sequence_number=candle.sequence_number,
            signal=Signal.SELL,
            confidence=round(raw_conf, 2),
            suggested_quantity=1,
            data_quality=data_quality,
            reason=f"Отрицательный импульс {meta['momentumPct']}% за {window_size} свечей.",
            processing_time_ms=0,
            metadata=meta,
        )

    # Импульс слабый — удерживаем позицию (HOLD)
    return Recommendation(
        recommendation_id=rec_id,
        source_event_id=candle.event_id,
        correlation_id=candle.correlation_id,
        run_id=candle.run_id,
        instrument=candle.ticker,
        sequence_number=candle.sequence_number,
        signal=Signal.HOLD,
        confidence=0.0,
        suggested_quantity=0,
        data_quality=data_quality,
        reason=f"Импульс {meta['momentumPct']}% в пределах нейтрального коридора.",
        processing_time_ms=0,
        metadata=meta,
    )
