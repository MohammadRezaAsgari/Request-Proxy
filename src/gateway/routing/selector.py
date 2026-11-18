from gateway.providers.registry import PROVIDERS, Provider


class RoundRobinSelector:
    def __init__(self, providers: list[Provider]):
        if not providers:
            raise ValueError("At least one provider is required")

        self.providers = providers
        self.current_index = 0

    def select(self) -> Provider:
        provider = self.providers[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.providers)
        return provider


selector = RoundRobinSelector(PROVIDERS)
