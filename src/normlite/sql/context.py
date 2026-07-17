from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from normlite.sql.elements import ColumnElement

class PlanningContext:
    def __init__(self):
        self.residual_where: Optional[ColumnElement] = None

