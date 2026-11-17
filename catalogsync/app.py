from pathlib import Path

from .database import Database
from .diagnostics import Diagnostics
from .ingest import Ingestor
from .publish import Publisher
from .query import CatalogQuery
from .registry import Registry
from .runs import Runs
from .profiles import Profiles
from .feed import DeltaInbox
from .provenance import Provenance
from .rejections import Rejections
from .file_feed import FileFeed


class Application:
    def __init__(self, home, request_policy=None):
        self.home = Path(home).resolve()
        self.home.mkdir(parents=True, exist_ok=True)
        self.database = Database(self.home / "catalog.sqlite")
        self.registry = Registry(self.database)
        self.runs = Runs(self.database)
        self.profiles = Profiles(self.database)
        self.ingest = Ingestor(
            self.database,
            self.home / "archive",
            state_dir=self.home,
            policy=request_policy,
        )
        self.inbox = DeltaInbox(self.database, self.ingest)
        self.query = CatalogQuery(self.database)
        self.publisher = Publisher(self.database, self.home / "datasets")
        self.diagnostics = Diagnostics(
            self.database, self.home / "archive", state_dir=self.home
        )
        self.provenance = Provenance(self)
        self.rejections = Rejections(self)
        self.file_feeds = FileFeed(self)
