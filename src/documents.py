from __future__ import annotations
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, Any, List, Optional
from pymongo.database import Database
from pymongo import ASCENDING

@dataclass
class RepositoryDocument:
    # collection name is configurable; pass it when saving
    name: str
    link: str
    owner_id: str
    content: Dict[str, str] = field(default_factory=dict)
    topics: List[str] = field(default_factory=list)
    readme_text: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "RepositoryDocument":
        return cls(
            name=d["name"],
            link=d["link"],
            owner_id=d.get("owner_id", ""),
            content=d.get("content", {}),
            topics=d.get("topics", []),
            readme_text=d.get("readme_text"),
        )

    def to_mongo(self) -> Dict[str, Any]:
        # drop Nones if desired
        doc = asdict(self)
        return {k: v for k, v in doc.items() if v is not None}

    def save(self, db: Database, collection_name: str) -> None:
        col = db[collection_name]
        # upsert on (owner_id, name)
        col.replace_one(
            {"owner_id": self.owner_id, "name": self.name},
            self.to_mongo(),
            upsert=True,
        )

    @staticmethod
    def create_indexes(db: Database, collection_name: str) -> None:
        col = db[collection_name]
        col.create_index([("owner_id", ASCENDING)])
        col.create_index([("owner_id", ASCENDING), ("name", ASCENDING)], unique=True)