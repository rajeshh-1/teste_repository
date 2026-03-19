from pathlib import Path


def _python_files(path: Path):
    return [p for p in path.glob("*.py") if p.is_file()]


def test_crypto_domain_does_not_import_sports():
    root = Path(__file__).resolve().parents[1] / "bot" / "crypto_updown"
    for file_path in _python_files(root):
        text = file_path.read_text(encoding="utf-8")
        assert "bot.sports" not in text, f"cross-domain import found in {file_path.name}"


def test_sports_domain_does_not_import_crypto():
    root = Path(__file__).resolve().parents[1] / "bot" / "sports"
    for file_path in _python_files(root):
        text = file_path.read_text(encoding="utf-8")
        assert "bot.crypto_updown" not in text, f"cross-domain import found in {file_path.name}"
