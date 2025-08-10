from pydantic import BaseModel, Field
from typing import Optional, Literal

# Modelo para os filtros do endpoint /cda/search

class CDASearchFilters(BaseModel):
    numCDA: Optional[str] = None
    minSaldo: Optional[float] = None
    maxSaldo: Optional[float] = None
    minAno: Optional[int] = None
    maxAno: Optional[int] = None
    natureza: Optional[str] = None
    
    sort_by: Optional[Literal['ano', 'valor']] = 'ano'
    sort_order: Optional[Literal['asc', 'desc']] = 'desc'
    
    limit: int = Field(100, gt=0, le=500)
    offset: int = Field(0, ge=0)

# Modelo para a resposta do endpoint de detalhes do devedor

class DevedorDetails(BaseModel):
    name: str
    tipo_pessoa: str
    documento: Optional[str] = None

# Modelo para a resposta do endpoint de montante acumulado

class MontanteAcumuladoItem(BaseModel):
    Percentil: int
    IPTU: Optional[float] = 0
    ISS: Optional[float] = 0
    Taxas: Optional[float] = 0
    Multas: Optional[float] = 0
    ITBI: Optional[float] = 0

    class Config:
        from_attributes = True