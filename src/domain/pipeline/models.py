from pydantic import BaseModel
from typing import List, Optional, Dict, Any

class SourceConfig(BaseModel):
    type: str # e.g., 'clickhouse', 'postgres', 'iceberg'
    database: str
    table: str

class ColumnMapping(BaseModel):
    source: Optional[str] = None
    target: str
    type: str
    expression: Optional[str] = None
    ast: Optional[Dict[str, Any]] = None

class TransformConfig(BaseModel):
    column_mapping: List[ColumnMapping]

class TargetConfig(BaseModel):
    type: str
    database: str
    table: str
    write_mode: str # 'upsert', 'delete_insert', 'scd_type2'
    primary_keys: Optional[List[str]] = None
    engine: Optional[str] = None
    order_by: Optional[List[str]] = None
    partition_by: Optional[List[str]] = None

class PipelineConfig(BaseModel):
    run_id: Optional[str] = None
    control_plane_url: Optional[str] = None
    pipeline_name: str
    source: SourceConfig
    transform: TransformConfig
    target: TargetConfig
