from dataclasses import dataclass

from gateway.config import settings


@dataclass(frozen=True)
class Provider:
    name: str
    base_url: str


PROVIDERS = [
    Provider(name="provider_a", base_url=settings.provider_a_url),
    Provider(name="provider_b", base_url=settings.provider_b_url),
    Provider(name="provider_c", base_url=settings.provider_c_url),
]
