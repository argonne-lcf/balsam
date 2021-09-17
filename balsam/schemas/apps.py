from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator

MAX_APP_SERIALIZED_SIZE = 262_144


class AppParameter(BaseModel):
    required: bool
    default: Optional[str]
    help: str = ""

    @validator("default")
    def is_required_or_has_default(cls, default_value: Optional[str], values: Dict[str, Any]) -> Optional[str]:
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
    recursive: bool = False

    @validator("local_path")
    def path_is_relative(cls, v: Path) -> Path:
        if v.is_absolute():
            raise ValueError("Cannot use absolute path")
        return v


class AppBase(BaseModel):
    site_id: int = Field(..., example=3, description="Site id at which this App is registered")
    name: str = Field(..., example="NWChemGeomOpt", description="Python AppDef class name")
    serialized_class: str = Field(..., description="Base64-encoded ApplicationDefinition payload")
    source_code: str = Field(..., description="App-introspected source code")
    description: str = Field("", example="NWChem7 geometry optimizer", description="The App class docstring")
    parameters: Dict[str, AppParameter] = Field(
        {},
        example={
            "input_file": {
                "required": False,
                "default": "input.nw",
                "help": "Path to input deck",
            }
        },
        description="Allowed parameters in the App command",
    )
    transfers: Dict[str, TransferSlot] = Field(
        {},
        example={
            "input_file": {
                "required": True,
                "direction": "in",
                "local_path": "input.nw",
                "description": "Input Deck",
                "recursive": "False",
            }
        },
        description="Allowed transfer slots in the App",
    )

    @validator("name")
    def is_valid_class_name(cls, v: str) -> str:
        if not v.isidentifier():
            raise ValueError(f"{v} is not a valid class name")
        return v

    @validator("serialized_class")
    def max_class_len(cls, v: str) -> str:
        if len(v) > MAX_APP_SERIALIZED_SIZE:
            raise AssertionError(f"Serialized App cannot be larger than {MAX_APP_SERIALIZED_SIZE}")
        return v


class AppCreate(AppBase):
    pass


class AppUpdate(AppBase):
    site_id: int = Field(None, example=3, description="Site id at which this App is registered")
    name: str = Field(None, example="NWChem7GeomOpt", description="Python AppDef class name")
    serialized_class: str = Field(None, description="Base64-encoded ApplicationDefinition payload")
    source_code: str = Field(None, description="App-introspected source code")


class AppOut(AppBase):
    class Config:
        orm_mode = True

    id: int = Field(..., example=234)


class PaginatedAppsOut(BaseModel):
    count: int
    results: List[AppOut]
