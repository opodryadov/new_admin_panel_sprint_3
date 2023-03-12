from typing import Optional

from pydantic import BaseModel


class GenreSchema(BaseModel):
    id: str
    name: str


class PersonSchema(BaseModel):
    id: str
    full_name: str


class FilmworkSchema(BaseModel):
    id: str
    imdb_rating: Optional[float]
    title: str
    description: Optional[str]
    genre: list[GenreSchema] = []
    actors_names: list[str] = []
    writers_names: list[str] = []
    directors_names: list[str] = []
    actors: list[PersonSchema] = []
    writers: list[PersonSchema] = []
    directors: list[PersonSchema] = []
