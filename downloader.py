#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import requests
import datetime
from database import DatabaseUtil
import shutil
from PIL import Image
from collections import defaultdict


class DownloaderException(Exception):
    pass


class Downloader:
    def get_local_sets_and_photos(self, download_path):
        set_folders = os.listdir(download_path)
        set_folders = [(int(parts[1][1]), parts[0]) for parts in [(folder, folder.split('_')) for folder in set_folders] if len(parts[1]) >= 3]
        if len(set([folder[0] for folder in set_folders])) != len(set_folders):
            raise DownloaderException('Sets contain duplicates.')

        set_id_to_set = dict(set_folders)
        photos = [(folder, os.listdir(os.path.join(download_path, folder))) for folder in set_id_to_set.values()]
        photos = [(folder[0], [(photo[:-5].split('_'), photo) for photo in folder[1] if photo[-5:] == '.jpeg']) for folder in photos]
        photos = [(folder[0], [(int(photo[0][1]), photo[1]) for photo in folder[1] if len(photo[0]) == 2]) for folder in photos]
        photos = [[(photo[0], (folder[0], photo[1])) for photo in folder[1]] for folder in photos]
        photos = [photo for folder in photos for photo in folder]
        photo_id_to_photo = defaultdict(list)
        for photo in photos:
            photo_id_to_photo[photo[0]].append(photo[1])

        return set_id_to_set, photo_id_to_photo

    def download_file(self, url, file_name):
        with open(file_name, "wb") as file:
            # get request
            response = requests.get(url)
            # write to file
            file.write(response.content)

    def download(self, download_path):
        download_path = os.path.expanduser(download_path)

        if not os.path.isdir(download_path):
            os.makedirs(download_path)

        set_id_to_set, photo_id_to_photo = self.get_local_sets_and_photos(download_path)

        photosets = DatabaseUtil.get_sets()
        sets_in_flickr = []
        for photoset in photosets:
            sets_in_flickr.append(photoset.photoset_id)
            photos = DatabaseUtil.get_set_photos(photoset.photoset_id)
            photoset_path = f"{photoset.title.replace(' ', '_')}_{photoset.photoset_id}"

            print(f"Ensuring that path '{photoset_path}' exists.")
            move = False
            if photoset.photoset_id in set_id_to_set:
                current_set_path = set_id_to_set[photoset.photoset_id]
                if current_set_path != photoset_path:
                    move = True

            photoset_path = os.path.join(download_path, photoset_path)
            if move:
                current_set_path = os.path.join(download_path, current_set_path)
                print(f"Renaming set folder '{current_set_path}' to '{photoset_path}'.")
                os.rename(current_set_path, photoset_path)
                set_id_to_set, photo_id_to_photo = self.get_local_sets_and_photos()
            elif not os.path.exists(photoset_path):
                print(f"Creating new photoset folder '{photoset_path}'.")
                os.makedirs(photoset_path)
            else:
                print(f"Photoset folder '{photoset_path}' already exists.")

            already_checked_photos = {}
            for photo in photos:
                photo_path = f"{photo.taken_timestamp.strftime('%Y%m%dT%H%M%S')}_{photo.photo_id}.jpeg"
                photo_path = os.path.join(photoset_path, photo_path)

                if photo.photo_id in already_checked_photos:
                    os.link(already_checked_photos[photo.photo_id], photo_path)
                    continue

                elif photo.photo_id in photo_id_to_photo:
                    current_photo_paths = photo_id_to_photo[photo.photo_id]
                    current_photo_path = current_photo_paths[0]
                    current_photo_path = os.path.join(download_path, current_photo_path[0], current_photo_path[1])
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
                        os.remove(current_photo_path)
                        # no continue here
                    elif current_photo_path != photo_path:
                        print(f"Moving photo '{current_photo_path}' to '{photo_path}'.")
                        os.rename(current_photo_path, photo_path)
                    else:
                        print(f"Photo id {photo.photo_id} already exists in path '{photo_path}'.")

                    for extra_photo in current_photo_paths[1:]:
                        extra_photo_path = os.path.join(download_path, extra_photo[0], extra_photo[1])
                        os.remove(extra_photo_path)
                    del photo_id_to_photo[photo.photo_id]

                    already_checked_photos[photo.photo_id] = photo_path
                    if not delete:
                        continue

                print(f"Downloading photo id {photo.photo_id} to path '{photo_path}'.")
                try:
                    self.download_file(photo.url_original, photo_path)
                except:
                    print(f"Failed to download photo from path '{photo.url_original}'.")
                    raise
                already_checked_photos[photo.photo_id] = photo_path

        print("Removing photos that no longer exists in Flickr.")
        for photo_id, photo_paths in photo_id_to_photo.items():
            for photo_path in photo_paths:
                photo_to_remove = os.path.join(download_path, photo_path[0], photo_path[1])
                print(f"Removing photo {photo_to_remove}.")
                os.remove(photo_to_remove)
        print("Removing photosets that no longer exists in Flickr.")
        for photoset_id, set_path in set_id_to_set.items():
            if photoset_id not in sets_in_flickr:
                set_to_remove = os.path.join(download_path, set_path)
                print(f"Removing photoset {set_to_remove}.")
                shutil.rmtree(set_to_remove)