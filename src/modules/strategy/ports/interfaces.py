from abc import ABC, abstractmethod

from ..core.entities import RecommendationDecision


class RecommendationPublisherPort(ABC):
    @abstractmethod
    def publish_recommendation(self, decision: RecommendationDecision) -> None:
        pass


class DeduplicationStorePort(ABC):
    @abstractmethod
    def is_seen(self, event_id: str) -> bool:
        pass

    @abstractmethod
    def mark_seen(self, event_id: str) -> None:
        pass
