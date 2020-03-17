#!/usr/bin/python
# -*- coding: utf-8 -*-

from flickr import Flickr
from database import Database
import argparse
import logging
import os
import shutil
import requests
import datetime
from PIL import Image
from collections import defaultdict


def parse_arguments():
    """
    Parse command line arguments
    :return: argparse.ArgumentParser object
    """
    parser = argparse.ArgumentParser(description="Flickr Organizer!")
    parser.add_argument('--photos', action='store_true', help="Update photos table")
    parser.add_argument('--sets', action='store_true', help="Update photo sets table")
    parser.add_argument('--ignore-photo-in-many-sets', action = 'store_true', help = "Do not issue a warning if a photo belongs to more than one set.")
    parser.add_argument('--ignore-photo-in-no-sets', action = 'store_true', help = "Do not issue a warning if a photo doesn't belongs to any set.")
    parser.add_argument('--delete-duplicates', action = 'store_true', help = "Delete duplicate photos.")
    parser.add_argument('--dry-run', action = 'store_true', help = "Combine with --delete-duplicates, will only show what would be done.")
    parser.add_argument('--order-sets', action = 'store_true', help = "Reorder sets by taken date")
    parser.add_argument('--download-path', type = str, help = "Download all photos from the Flickr Account to given local directory")
    return parser.parse_args()


logging.basicConfig(filename='connector.log',filemode='w',level=logging.DEBUG,format='%(asctime)s %(message)s')
logging.debug('Get user photos')

db = Database()
flickr = Flickr(db)

failedphoto = None

def escape_apostrophe(str):
    return str.replace('\'','\'\'')

def remove_extension(filename):
    lower = filename.lower()
    if lower.endswith('.jpeg'):
        return filename[:-5]
    if lower.endswith('.jpg') or lower.endswith('.mov') or lower.endswith('.avi'):
        return filename[:-4]
    return filename


def order_photosets_by_taken_date():
    logging.debug('Get photoset ids ordered by photo taken date desc.')
    sql = """select ps.id
    from f_photoset ps
    inner join f_photosetphoto psp on ps.id = psp.photosetId
    left join f_photosetnodate nd on ps.id = nd.id
    inner join f_photo p on psp.photoId = p.id
    group by ps.id, ps.title order by max(case when nd.id is not null then cast('1900-01-01' as datetime) else p.taken end) desc"""

    setids = db.get_int_list_from_database(sql)
    setids = [str(i) for i in setids]
    logging.debug('Retrieved %s photoset ids.' % len(setids))
    flickr.Photoset().orderSets(photoset_ids=','.join(setids))

    logging.debug('Ordering photos inside sets. Getting list of sets.')
    photosets = flickr.get_photosets()
    for photoset in photosets:
        logging.debug('Getting photos of set %s and ordering them by taken date.' % photoset.title)
        sql = """select p.id from f_photo p
                 inner join f_photosetphoto psp on p.id = psp.photoId
                 where psp.photosetId = %s
                 order by p.taken asc""" % photoset.id
        photoids = db.get_int_list_from_database(sql)
        photoids = [str(i) for i in photoids]
        try:
            photoset.reorderPhotos(photo_ids=','.join(photoids))
        except flickr.FlickrError as e:
            logging.warn("Could not reorder photoset %s.", photoset.id)

             
def move_date_taken(sql, photoids, dtfix, giventime):
    if not sql is None:
        logging.debug('Getting photo ids from database.')
        photoids = db.get_int_list_from_database(sql)

    if not isinstance(photoids, list):
        photoids = photoids.split(',')

    photoids = [int(i) for i in photoids]
    setidtoset, photoidtophoto = get_local_sets_and_photos()
    # Ensure that all photos have been loaded by constructing a dictionary.

    photoidtolocalphoto = dict([[id, os.path.join(photorootdir, photoidtophoto[id][0][0], photoidtophoto[id][0][1])] for id in photoids])
    datekeys = ['Exif.Photo.DateTimeOriginal', 'Exif.Photo.DateTimeDigitized']
    photoidtodatetime = dict()
        
    for id in photoidtolocalphoto:
        img = Image.open(photoidtolocalphoto[id])
        metadata = img._getexif()
        #metadata.read()
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
                raise('No take date found from photo')
    for id in photoidtolocalphoto:
        logging.debug('Changing taken dates of photo %s %s.' % (id, photoidtolocalphoto[id]))
        img = Image.open(photoidtolocalphoto[id])
        metadata = img._getexif()

        dt = photoidtodatetime[id]
        if giventime is None:
            dt = dt + dtfix
        for datekey in datekeys:
            if datekey in metadata.exif_keys:
                metadata[datekey].value = dt
            else:
                raise NotImplementedError("Broken in Python 3 conversion!")
            metadata.write()

    for id in photoidtolocalphoto:
        logging.debug('Replacing photo %s ''%s''.' % (id, photoidtolocalphoto[id]))
        flickr.Upload.replace(photo_id = id, photo_file = photoidtolocalphoto[id])

arguments = parse_arguments()

if arguments.sets:
    flickr.update_photosets()
if arguments.photos:
    ignore_photo_in_many_sets = arguments.ignore_photo_in_many_sets
    ignore_photo_in_no_sets = arguments.ignore_photo_in_no_sets
    flickr.update_photos(ignore_photo_in_many_sets, ignore_photo_in_no_sets)
if arguments.delete_duplicates:
    dry_run = arguments.dry_run
    flickr.delete_duplicates(dry_run)
if arguments.order_sets:
    dry_run = arguments.dry_run
    flickr.fix_ordering_of_sets(dry_run)
if arguments.download_path is not None:
    flickr.download(arguments.download_path)
#if ('order' in sys.argv):
#    order_photosets_by_taken_date()
#if ('download' in sys.argv):
#    download()

# if ('move' in sys.argv):
#     sql = """select p.id
#     from f_photo p
#     inner join f_photosetphoto psp
#     on p.id = psp.photoid
#     where psp.photosetid = 72157646713519388
#     and left(p.title,3)='DSC'"""
#
    #move_date_taken(None, '15157778368', datetime.timedelta(minutes=38), None)
    #move_date_taken(None, '14923073599,14923125960', None, None, None, None, datetime.datetime(2014, 8, 16, 13, 15))
