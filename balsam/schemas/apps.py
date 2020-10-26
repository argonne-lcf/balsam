from pathlib import Path
from pydantic import BaseModel, validator, Field
from typing import Dict, Optional, List
from enum import Enum


class AppParameter(BaseModel):
    required: bool
    default: Optional[str]
    help: str = ""

    @validator("default")
    def is_required_or_has_default(cls, default_value, values):
        if values.get("required", True):
            if default_value:
                raise ValueError("cannot be required and have default")
        else:
            if not default_value:
                raise ValueError("optional param needs a default")
        return default_value


class TransferDirection(str, Enum):
    stage_in = "in"
    stage_out = "out"


class TransferSlot(BaseModel):
    required: bool
    direction: TransferDirection
    local_path: Path
    description: str = ""

    @validator("local_path")
    def path_is_relative(cls, v):
        if v.is_absolute():
            raise ValueError("Cannot use absolute path")
        return v


class AppBase(BaseModel):
    site_id: int = Field(..., example=3)
    description: str = Field("", example="NWChem7 geometry optimizer")
    class_path: str = Field(..., example="nwchem7.GeomOpt")
    parameters: Dict[str, AppParameter] = Field(
        {},
        example={
            "input_file": {
                "required": False,
                "default": "input.nw",
                "help": "Path to input deck",
            }
        },
    )
    transfers: Dict[str, TransferSlot] = Field(
        {},
        example={
            "input_file": {
                "required": True,
                "direction": "in",
                "local_path": "input.nw",
                "description": "Input Deck",
            }
        },
    )
    last_modified: Optional[float] = Field(None)

    @validator("class_path")
    def is_class_path(cls, v: str):
        if not all(s.isidentifier() for s in v.split(".")):
            raise ValueError(f"{v} is not a valid class path")
        return v


class AppCreate(AppBase):
    pass


class AppUpdate(AppBase):
    site_id: int = Field(None, example=3)
    class_path: str = Field(None, example="nwchem7.GeomOpt")


class AppOut(AppBase):
    class Config:
        orm_mode = True

    id: int


class PaginatedAppsOut(BaseModel):
    count: int
    results: List[AppOut]
