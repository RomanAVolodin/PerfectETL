import datetime

from pydantic import BaseModel


class IdMixIn(BaseModel):
    id: str


class UpdatedAtMixin(BaseModel):
    updated_at: str | datetime.datetime
