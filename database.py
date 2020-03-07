#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
from peewee import *

db = SqliteDatabase(os.environ['SQLITE_DATABASE'])


class BaseModel(Model):
    class Meta:
        database = db


class FlickrPhoto(BaseModel):
    photo_id = IntegerField(index=True, unique=True)
    title = TextField()
    original_format = TextField()
    tags = TextField()
    machine_tags = TextField()
    media = TextField()
    media_status = TextField()
    view_count = IntegerField()
    taken_timestamp = DateTimeField()
    uploaded_timestamp = DateTimeField()
    updated_timestamp = DateTimeField()
    url_original = TextField()
    width = IntegerField()
    height = IntegerField()


class FlickrPhotoSet(BaseModel):
    photoset_id = IntegerField(index=True, unique=True)
    title = TextField()
    created_timestamp = DateTimeField()
    comment_count = IntegerField()
    view_count = IntegerField()
    photo_count = IntegerField()
    video_count = IntegerField()
    updated_timestamp = DateTimeField()
    created_timestamp = DateTimeField()


#photoset nodate ?
class FlickrPhotoSetPhoto(BaseModel):
    photo_id = ForeignKeyField(FlickrPhoto)
    photoset_id = ForeignKeyField(FlickrPhotoSet)


class LocalPhoto(BaseModel):
    file_path = TextField(index=True, unique=True)
    title = TextField()
    width = IntegerField()
    height = IntegerField()
    plain_timestamp = DateTimeField()
    digitized_timestamp = DateTimeField()
    original_timestamp = DateTimeField()
    unique_id = TextField()
    make = TextField()
    model = TextField(null=True)
    orientation = IntegerField(null=True)


class Database():
    def __init__(self):
        self.db = db
        self.db.connect()
        self.db.create_tables([FlickrPhoto, FlickrPhotoSet, FlickrPhotoSetPhoto, LocalPhoto])


