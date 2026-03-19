"""Sports domain package."""

from .config import SportsRuntimeConfig, build_sports_runtime_config, validate_sports_startup
from .matching import SportsMarketSnapshot, SportsMatchResult, validate_sports_match

__all__ = [
    "SportsMarketSnapshot",
    "SportsMatchResult",
    "SportsRuntimeConfig",
    "build_sports_runtime_config",
    "validate_sports_match",
    "validate_sports_startup",
]
