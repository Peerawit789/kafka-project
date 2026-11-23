import datetime
from sqlalchemy import Column, String, Integer, Text, DateTime, JSON
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional

from database import Base


class JobModel(Base):
    __tablename__ = "jobs"

    job_id = Column(String(64), primary_key=True, index=True)
    router_id = Column(String(32), nullable=False, index=True)
    action = Column(String(64), nullable=False)
    params = Column(JSON, default=dict)
    requested_by = Column(String(64), default="anonymous")
    status = Column(String(32), default="pending", index=True)  # pending, running, success, failed
    output = Column(Text, nullable=True)
    error = Column(Text, nullable=True)
    attempts = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)

    def to_dict(self):
        return {
            "job_id": self.job_id,
            "router_id": self.router_id,
            "action": self.action,
            "params": self.params or {},
            "requested_by": self.requested_by,
            "status": self.status,
            "output": self.output,
            "error": self.error,
            "attempts": self.attempts,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
        }


# Pydantic Schemas
class JobCreateRequest(BaseModel):
    router_id: str
    action: str
    params: Dict[str, Any] = Field(default_factory=dict)
    requested_by: Optional[str] = "web-user"


class JobCreateResponse(BaseModel):
    job_id: str
    router_id: str
    action: str
    status: str
    created_at: str
    message: str


class JobDetailResponse(BaseModel):
    job_id: str
    router_id: str
    action: str
    params: Dict[str, Any]
    requested_by: str
    status: str
    output: Optional[str]
    error: Optional[str]
    attempts: int
    created_at: Optional[str]
    started_at: Optional[str]
    finished_at: Optional[str]
