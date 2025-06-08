from pydantic import BaseModel
from uuid import UUID

class SkillsCreatedSummary(BaseModel):
    org_id: UUID
    org_name: str
    year_month: str
    created_skills: int