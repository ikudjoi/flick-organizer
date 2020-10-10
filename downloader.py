#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import requests
from database import DatabaseUtil
import shutil
from PIL import Image
from collections import defaultdict


class DownloaderException(Exception):
    pass


class Downloader:
    def __init__(self, download_path, dry_run):
        self.download_path = os.path.expanduser(download_path)
        self.dry_run = dry_run

    def get_local_sets_and_photos(self):
        if not os.path.isdir(self.download_path):
            return {}, {}

        set_folders = os.listdir(self.download_path)
        set_folders = [(int(parts[1][-1]), parts[0]) for parts in [(folder, folder.split('_')) for folder in set_folders] if len(parts[1]) >= 2]
        if len(set([folder[0] for folder in set_folders])) != len(set_folders):
            raise DownloaderException('Sets contain duplicates.')

        set_id_to_set = dict(set_folders)
        photos = [(folder, os.listdir(os.path.join(self.download_path, folder))) for folder in set_id_to_set.values()]
        photos = [(folder[0], [(photo[:-5].split('_'), photo) for photo in folder[1] if photo[-5:] == '.jpeg']) for folder in photos]
        photos = [(folder[0], [(int(photo[0][1]), photo[1]) for photo in folder[1] if len(photo[0]) == 2]) for folder in photos]
        photos = [[(photo[0], (folder[0], photo[1])) for photo in folder[1]] for folder in photos]
        photos = [photo for folder in photos for photo in folder]
        photo_id_to_photo = defaultdict(list)
        for photo in photos:
            photo_id_to_photo[photo[0]].append(photo[1])

        return set_id_to_set, photo_id_to_photo

    @staticmethod
    def download_file(url, file_name):
        with open(file_name, "wb") as file:
            # get request
            response = requests.get(url)
            # write to file
            file.write(response.content)

    def download(self):
        if not os.path.isdir(self.download_path):
            print(f"Create download path")
            if not self.dry_run:
                os.makedirs(self.download_path)

        set_id_to_set, photo_id_to_photo = self.get_local_sets_and_photos()

        photo_sets = DatabaseUtil.get_sets()
        sets_in_flickr = []
        for photo_set in photo_sets:
            sets_in_flickr.append(photo_set.photoset_id)
            photos = DatabaseUtil.get_set_photos(photo_set.photoset_id)
            photo_set_path = f"{photo_set.title.replace(' ', '_')}_{photo_set.photoset_id}"

            print(f"Ensuring that path '{photo_set_path}' exists.")
            move = False
            if photo_set.photoset_id in set_id_to_set:
                current_set_path = set_id_to_set[photo_set.photoset_id]
                if current_set_path != photo_set_path:
                    move = True

            photo_set_path = os.path.join(self.download_path, photo_set_path)
            if move:
                current_set_path = os.path.join(self.download_path, current_set_path)
                print(f"Renaming set folder '{current_set_path}' to '{photo_set_path}'.")
                if not self.dry_run:
                    os.rename(current_set_path, photo_set_path)
                    set_id_to_set, photo_id_to_photo = self.get_local_sets_and_photos()
            elif not os.path.exists(photo_set_path):
                print(f"Creating new photo set folder '{photo_set_path}'.")
                if not self.dry_run:
                    os.makedirs(photo_set_path)
            else:
                print(f"Photo set folder '{photo_set_path}' already exists.")

            already_checked_photos = {}
            for photo in photos:
                file_extension = os.path.splitext(photo.url_original)[1]
                # Favor jpeg over jpg as file extension.
                file_extension = file_extension.replace('.jpg', '.jpeg')
                photo_path = f"{photo.taken_timestamp.strftime('%Y%m%dT%H%M%S')}_{photo.photo_id}{file_extension}"
                photo_path = os.path.join(photo_set_path, photo_path)

                if photo.photo_id in already_checked_photos:
                    if not self.dry_run:
                        os.link(already_checked_photos[photo.photo_id], photo_path)
                    continue

                elif photo.photo_id in photo_id_to_photo:
                    current_photo_paths = photo_id_to_photo[photo.photo_id]
                    current_photo_path = current_photo_paths[0]
                    current_photo_path = os.path.join(self.download_path, current_photo_path[0], current_photo_path[1])
                    delete = False
                    try:
                        im = Image.open(current_photo_path)
                        im.load()
                        (width, height) = im.size
                        if (photo.width != width) or (photo.height != height):
                            print(f"Found photo with unmatching size '{current_photo_path}' (local w{width} x h{height}, flickr w{photo.width} x h{photo.height}")
                            delete = True
                    except IOError:
                        print(f"Found unreadable photo '{current_photo_path}'")
                        delete = True
                    if delete:
                        if not self.dry_run:
                            os.remove(current_photo_path)
                        # no continue here
                    elif current_photo_path != photo_path:
                        print(f"Moving photo '{current_photo_path}' to '{photo_path}'.")
                        if not self.dry_run:
                            os.rename(current_photo_path, photo_path)
                    else:
                        print(f"Photo id {photo.photo_id} already exists in path '{photo_path}'.")

                    for extra_photo in current_photo_paths[1:]:
                        print(f"Removing extra photo '{extra_photo}'.")
                        extra_photo_path = os.path.join(self.download_path, extra_photo[0], extra_photo[1])
                        if not self.dry_run:
                            os.remove(extra_photo_path)
                    del photo_id_to_photo[photo.photo_id]

                    already_checked_photos[photo.photo_id] = photo_path
                    if not delete:
                        continue

                print(f"Downloading photo id {photo.photo_id} to path '{photo_path}'.")
                try:
                    if not self.dry_run:
                        self.download_file(photo.url_original, photo_path)
                except Exception as ex:
                    print(f"Failed to download photo from path '{photo.url_original}', exception {ex}.")
                    raise
                already_checked_photos[photo.photo_id] = photo_path

        print("Removing photos that no longer exists in Flickr.")
        for photo_id, photo_paths in photo_id_to_photo.items():
            for photo_path in photo_paths:
                photo_to_remove = os.path.join(self.download_path, photo_path[0], photo_path[1])
                print(f"Removing photo {photo_to_remove}.")
                if not self.dry_run:
                    os.remove(photo_to_remove)
        print("Removing photosets that no longer exists in Flickr.")
        for photo_set_id, set_path in set_id_to_set.items():
            if photo_set_id not in sets_in_flickr:
                set_to_remove = os.path.join(self.download_path, set_path)
                print(f"Removing photoset {set_to_remove}.")
                if not self.dry_run:
                    shutil.rmtree(set_to_remove)

    def move_date_taken(self, photo_ids, timedelta_parameters):
        giventime = None
        photo_ids = [int(i) for i in photo_ids.split(',')]
        set_id_to_set, photo_id_to_photo = self.get_local_sets_and_photos()

        for photo_id in photo_ids:
            # Check if there's a downloaded copy of the requested photo
            if photo_id in photo_id_to_photo:
                continue

            photo = FlickrPhoto.select()
        query = User.select().where(User.active == True).order_by(User.username)

        # Ensure that all photos have been loaded by constructing a dictionary.

        photo_id_to_local_photo = dict(
            [[id, os.path.join(self.download_path, photo_id_to_photo[id][0][0], photo_id_to_photo[id][0][1])] for id in photo_ids])
        datekeys = ['Exif.Photo.DateTimeOriginal', 'Exif.Photo.DateTimeDigitized']
        photoidtodatetime = dict()

        for id, photo_path in photo_id_to_local_photo.items():
            img = Image.open(photo_path)
            metadata = img._getexif()
            # metadata.read()
            for datekey in datekeys:
                if datekey in metadata.exif_keys:
                    dt = metadata[datekey].value
                    if id not in photoidtodatetime:
                        photoidtodatetime[id] = dt
                    elif dt != photoidtodatetime[id]:
                        raise Exception('Different dates in photo.')
            if id not in photoidtodatetime:
                if giventime is not None:
                    photoidtodatetime[id] = giventime
                else:
                    raise ('No take date found from photo')
        for id, photo_path in photo_id_to_local_photo.items():
            print(f"Changing taken dates of photo {id} {photo_path}.")
            img = Image.open(photo_path)
            metadata = img._getexif()

            dt = photoidtodatetime[id]
            if giventime is None:
                dt = dt + timedelta_parameters
            for datekey in datekeys:
                if datekey in metadata.exif_keys:
                    metadata[datekey].value = dt
                else:
                    raise NotImplementedError("Broken in Python 3 conversion!")
                metadata.write()

        for id, photo_path in photo_id_to_local_photo.items():
            print(f"Replacing photo {id} '{photo_path}'.")
            #flickr.Upload.replace(photo_id=id, photo_file=photoidtolocalphoto[id])
