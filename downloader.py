#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import requests
import datetime
from retry import retry
from collections import defaultdict

class DownloaderException(Exception):
    pass


class Downloader:
    def __init__(self, db, flickr, local_photo_root_path):
        self.db = db
        self.flickr = flickr
        self.local_photo_root_path = local_photo_root_path

    def get_local_sets_and_photos(self):
        set_folders = os.listdir(self.local_photo_root_path)
        set_folders = [(int(parts[1][1]), parts[0]) for parts in [(folder, folder.split('_')) for folder in set_folders] if len(parts[1]) >= 3]
        if len(set([folder[0] for folder in set_folders])) != len(set_folders):
            raise DownloaderException('Sets contain duplicates.')

        set_id_to_set = dict(set_folders)
        photos = [(folder, os.listdir(os.path.join(self.local_photo_root_path, folder))) for folder in set_id_to_set.values()]
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


    def download(self):
        set_id_to_set, photo_id_to_photo = self.get_local_sets_and_photos()

        photosets = self.db.get_sets()
        sets_in_flickr = []
        for photoset in photosets:
            sets_in_flickr.append(photoset.id)
            photos = self.db.get_set_photos(photoset.id)
            photoset_date = min([photo.taken_timestamp for photo in photos])
            photoset_path = f"{photoset.title}_{photoset.id}"

            logging.debug(u'Ensuring that path "%s" exists.' % photosetpath)
            move = False
            if int(photoset.id) in setidtoset:
                currentsetpath = setidtoset[int(photoset.id)]
                if currentsetpath != photosetpath:
                    move = True

            photosetpath = os.path.join(photorootdir, photosetpath)
            if move:
                currentsetpath = os.path.join(photorootdir, currentsetpath)
                logging.debug(u'Renaming set folder %s to "%s".' % (currentsetpath, photosetpath))
                os.rename(currentsetpath, photosetpath)
                setidtoset, photoidtophoto = get_local_sets_and_photos()
            elif not os.path.exists(photosetpath):
                logging.debug(u'Creating new photoset folder "%s".' % photosetpath)
                os.makedirs(photosetpath)
            else:
                logging.debug(u'Photoset folder "%s" already exists.' % photosetpath)

            alreadycheckedphotos = dict()
            for photo in photos:
                photopath = '%s_%s.jpeg' % (datetime.datetime.strptime(photo.datetaken).strftime('%Y%m%d%H%M%S'), photo.id)
                photopath = os.path.join(photosetpath, photopath)

                if photo.id in alreadycheckedphotos:
                    os.link(alreadycheckedphotos[photo.id], photopath)
                    continue

                elif int(photo.id) in photoidtophoto:
                    currentphotopaths = photoidtophoto[int(photo.id)]
                    currentphotopath = currentphotopaths[0]
                    currentphotopath = os.path.join(photorootdir, currentphotopath[0], currentphotopath[1])
                    delete = False
                    try:
                        im = Image.open(currentphotopath)
                        im.load()
                        (width, height) = im.size
                        if ((int(photo.o_width) != int(width)) or (int(photo.o_height) != int(height))):
                            logging.debug(u'Found photo with unmatching size "%s" (local w%s x h%s, flickr w%s x h%s).' % \
                                          (currentphotopath, width, height, photo.o_width, photo.o_height))
                            delete = True
                    except (IOError):
                        logging.debug(u'Found unreadable photo "%s".' % (currentphotopath))
                        delete = True
                    if delete:
                        os.remove(currentphotopath)
                        # no continue here
                    elif currentphotopath != photopath:
                        logging.debug(u'Moving photo "%s to "%s".' % (currentphotopath, photopath))
                        os.rename(currentphotopath, photopath)
                    else:
                        logging.debug(u'Photo id %s already exists in path "%s".' % (photo.id, photopath))

                    for extraphoto in currentphotopaths[1:]:
                        extraphotopath = os.path.join(photorootdir, extraphoto[0], extraphoto[1])
                        os.remove(extraphotopath)
                    del photoidtophoto[int(photo.id)]

                    alreadycheckedphotos[photo.id] = photopath
                    if not delete:
                        continue

                logging.debug(u'Downloading photo id %s to path "%s".' % (photo.id, photopath))
                try:
                    download_file(photo.url_o, photopath)
                except (Exception):
                    logging.debug(u'Failed to download photo from path "%s". Trying again.' % (photo.url_o))
                except:
                    logging.debug(u'Failed to download photo from path "%s".' % (photo.url_o))
                    raise
                alreadycheckedphotos[photo.id] = photopath

        logging.debug(u'Removing photos that no longer exists in Flickr.')
        for photoid in photoidtophoto:
            for photopath in photoidtophoto[photoid]:
                phototoremove = os.path.join(photorootdir, photopath[0], photopath[1])
                logging.debug(u'Removing photo %s.' % (phototoremove))
                os.remove(phototoremove)
        logging.debug(u'Removing photosets that no longer exists in Flickr.')
        for photosetid in setidtoset:
            if photosetid not in setsinflickr:
                settoremove = os.path.join(photorootdir, setidtoset[photosetid])
                logging.debug(u'Removing photoset %s.' % (settoremove))
                shutil.rmtree(settoremove)