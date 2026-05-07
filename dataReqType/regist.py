from typing import List

from pydantic import BaseModel


class RegistModel(BaseModel):
    id : str
    pw : str
    user_name : str
    email : str
    phone_number : str
    keyword : List[str]