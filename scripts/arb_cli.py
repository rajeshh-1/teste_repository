import sys
import warnings
from pathlib import Path
from typing import Optional

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.crypto_cli import parse_args as _parse_args
from scripts.crypto_cli import validate_from_namespace as _validate_from_namespace
from scripts.crypto_cli import main as _crypto_main


warnings.warn("DEPRECATED: use scripts/crypto_cli.py", UserWarning, stacklevel=2)


def parse_args(argv: Optional[list[str]] = None):
    return _parse_args(argv)


def validate_from_namespace(args):
    return _validate_from_namespace(args)


def main(argv: Optional[list[str]] = None) -> int:
    return _crypto_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
