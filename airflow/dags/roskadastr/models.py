from pydantic import BaseModel, Json
from pendulum import DateTime, parse, datetime, Date


class Request(BaseModel):
    request_id: int
    request_date: DateTime
    request_reg_num: str
    request_type_id: int
    request_name: str
    request_description: str


class RequestFile(BaseModel):
    request_file_id: int
    request_file_name: str
    request_file_path: str
    request_id: int
    request_content: Json
    request_options: Json
    request_description: str


class Response(BaseModel):
    response_id: int
    response_name: str
    response_date: DateTime
    response_reg_num: str
    response_file_description: str
    response_type_id: int

