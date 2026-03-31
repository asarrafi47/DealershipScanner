"""Allow: python -m SCRAPING (same as python -m SCRAPING.cli)."""
from SCRAPING.cli import main
import sys

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
