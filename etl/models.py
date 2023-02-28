from typing import Optional

from pydantic import BaseModel


class PersonSchema(BaseModel):
    id: str
    name: str


class FilmworkSchema(BaseModel):
    id: str
    imdb_rating: Optional[float]
    genre: list[str]
    title: str
    description: Optional[str]
    actors_names: list[str] = []
    writers_names: list[str] = []
    director: list[PersonSchema] = []
    actors: list[PersonSchema] = []
    writers: list[PersonSchema] = []
